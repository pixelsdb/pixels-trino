/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino;

import com.alibaba.fastjson.JSON;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.state.StateWatcher;
import io.pixelsdb.pixels.common.turbo.SimpleOutput;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.impl.PixelsTupleDomainPredicate;
import io.pixelsdb.pixels.trino.split.PixelsFileSplit;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @author guodong
 * @author tao
 */
class PixelsFilePageSource implements PixelsPageSource
{
    private static final Logger logger = Logger.get(PixelsFilePageSource.class);
    private final int BatchSize;
    private final PixelsFileSplit split;
    private final List<PixelsColumnHandle> columns;
    private final PixelsTransactionHandle transactionHandle;
    private final String[] includeCols;
    private final Storage storage;
    private final PixelsCacheReader cacheReader;
    private final PixelsFooterCache footerCache;
    private final CompletableFuture<?> blocked;
    private final int numColumnToRead;
    private final Optional<TableScanFilter> filter;
    private final Bitmap filtered;
    private final Bitmap tmp;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private long completedBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;
    private PixelsReaderOption option;
    private int batchId;

    public PixelsFilePageSource(PixelsFileSplit split, List<PixelsColumnHandle> columnHandles, PixelsTransactionHandle transactionHandle,
                                Storage storage, List<MemoryMappedFile> cacheFiles, List<MemoryMappedFile> indexFiles, int swapZoneNum,
                                PixelsFooterCache pixelsFooterCache)
    {
        this.split = split;
        this.transactionHandle = transactionHandle;
        this.storage = storage;
        this.columns = columnHandles;
        this.includeCols = new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            if (split.getReadSynthColumns())
            {
                // Use the synthetic column name to access the join or aggregation result.
                includeCols[i] = columns.get(i).getSynthColumnName();
            } else
            {
                includeCols[i] = columns.get(i).getColumnName();
            }
        }
        this.numColumnToRead = columnHandles.size();
        this.footerCache = pixelsFooterCache;
        this.batchId = 0;
        this.closed = false;
        this.BatchSize = PixelsTrinoConfig.getBatchSize();
        this.filtered = new Bitmap(this.BatchSize, true);
        this.tmp = new Bitmap(this.BatchSize, false);

        this.cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFiles(cacheFiles, swapZoneNum)
                .setIndexFiles(indexFiles.isEmpty() ? null : indexFiles.subList(0, indexFiles.size() - 1),
                        indexFiles.isEmpty() ? null : indexFiles.get(indexFiles.size() - 1))
                .build();

        if (split.getFromServerlessOutput())
        {
            this.filter = Optional.empty();
            this.blocked = new CompletableFuture<>();
            String stateKey = PixelsTrinoConfig.getOutputStateKeyPrefix(
                    split.getTransId(), Optional.of(split.getSchemaTableName())) + split.getSplitId();
            StateWatcher stateWatcher = new StateWatcher(stateKey);
            stateWatcher.onStateUpdateOrExist((key, value) ->
            {
                SimpleOutput simpleOutput = requireNonNull(
                        JSON.parseObject(value, SimpleOutput.class), "output is null");
                if (!simpleOutput.isSuccessful())
                {
                    this.blocked.cancel(true);
                    throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_EXECUTION_CF_ERROR,
                            "cloud function request " + simpleOutput.getRequestId() +
                                    " returns error. transaction id: " + split.getTransId() +
                                    ", error message: " + simpleOutput.getErrorMessage());
                } else
                {
                    // PIXELS-643： trim the output paths for the last few scan workers.
                    split.trimForServerlessOutput(simpleOutput.getNumOutputs());
                    readFirstPath();
                    this.blocked.complete(null);
                    logger.debug("cloud function request " + simpleOutput.getRequestId() + " is successful");
                }
            });
        } else
        {
            if (split.getConstraint().getDomains().isPresent())
            {
                TableScanFilter scanFilter = PixelsSplitManager.createTableScanFilter(
                        split.getSchemaName(), split.getTableName(),
                        includeCols, split.getConstraint());
                this.filter = Optional.of(scanFilter);
            } else
            {
                this.filter = Optional.empty();
            }
            readFirstPath();
            this.blocked = NOT_BLOCKED;
        }
    }

    private void readFirstPath()
    {
        if (split.isEmpty())
        {
            this.close();
            return;
        }

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.enableEncodedColumnVector(true);
        this.option.readIntColumnAsIntVector(true);
        this.option.includeCols(includeCols);
        this.option.rgRange(split.getRgStart(), split.getRgLength());
        this.option.transId(split.getTransId());
        this.option.transTimestamp(transactionHandle.getTimestamp());

        if (split.getConstraint().getDomains().isPresent() && !split.getColumnOrder().isEmpty())
        {
            Map<PixelsColumnHandle, Domain> domains = split.getConstraint().getDomains().get();
            List<PixelsTupleDomainPredicate.ColumnReference<PixelsColumnHandle>> columnReferences =
                    new ArrayList<>(domains.size());
            for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet())
            {
                PixelsColumnHandle column = entry.getKey();
                String columnName = column.getColumnName();
                int columnOrdinal = split.getColumnOrder().indexOf(columnName);
                columnReferences.add(
                        new PixelsTupleDomainPredicate.ColumnReference<>(
                                column,
                                columnOrdinal,
                                column.getColumnType()));
            }
            PixelsPredicate predicate = new PixelsTupleDomainPredicate<>(split.getConstraint(), columnReferences);
            this.option.predicate(predicate);
        }

        try
        {
            if (this.storage != null)
            {
                this.pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setStorage(this.storage)
                        .setPath(split.getPath())
                        .setEnableCache(split.getCached())
                        .setCacheOrder(split.getCacheOrder())
                        .setPixelsCacheReader(cacheReader)
                        .setPixelsFooterCache(footerCache)
                        .build();
                if (this.pixelsReader.getRowGroupNum() <= this.option.getRGStart())
                {
                    /**
                     * As PixelsSplitManager does not check the exact number of row groups
                     * in the file, the start row group index might be invalid. in this case,
                     * we can simply close this page source.
                     */
                    this.close();
                } else
                {
                    this.recordReader = this.pixelsReader.read(this.option);
                }
            } else
            {
                logger.error("pixelsReader error: storage handler is null");
                throw new IOException("pixelsReader error: storage handler is null.");
            }
        } catch (IOException e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR,
                    "create Pixels reader error.", e);
        }
    }

    private synchronized boolean readNextPath()
    {
        try
        {
            if (this.split.nextPath())
            {
                closeReader();
                if (this.storage != null)
                {
                    this.pixelsReader = PixelsReaderImpl
                            .newBuilder()
                            .setStorage(this.storage)
                            .setPath(split.getPath())
                            .setEnableCache(split.getCached())
                            .setCacheOrder(split.getCacheOrder())
                            .setPixelsCacheReader(this.cacheReader)
                            .setPixelsFooterCache(this.footerCache)
                            .build();
                    this.option.rgRange(split.getRgStart(), split.getRgLength());
                    if (this.pixelsReader.getRowGroupNum() <= this.option.getRGStart())
                    {
                        /**
                         * As PixelsSplitManager does not check the exact number of row groups
                         * in the file, the start row group index might be invalid. In this case,
                         * we can simply return false, and the page source will be closed outside.
                         */
                        return false;
                    }
                    this.recordReader = this.pixelsReader.read(this.option);
                } else
                {
                    logger.error("pixelsReader error: storage handler is null");
                    throw new IOException("pixelsReader error: storage handler is null");
                }
                return true;
            } else
            {
                return false;
            }
        } catch (Exception e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR, "read next path error.", e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        if (closed)
        {
            return this.completedBytes;
        }
        return this.completedBytes + (recordReader != null ? recordReader.getCompletedBytes() : 0);
    }

    @Override
    public long getReadTimeNanos()
    {
        if (closed)
        {
            return readTimeNanos;
        }
        return this.readTimeNanos + (recordReader != null ? recordReader.getReadTimeNanos() : 0);
    }

    @Override
    public long getMemoryUsage()
    {
        /**
         * PIXELS-113:
         * I am still not sure show the result of this method are used by Presto.
         * Currently, we return the cumulative memory usage. However this may be
         * inappropriate.
         * I tested about ten queries on test_1187, there was no problem, but
         * TODO: we still need to be careful about this method in the future.
         */
        if (closed)
        {
            return memoryUsage;
        }
        return this.memoryUsage + (recordReader != null ? recordReader.getMemoryUsage() : 0);
    }

    @Override
    public boolean isFinished()
    {
        return this.closed;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return this.blocked;
    }

    @Override
    public Page getNextPage()
    {
        if (!this.blocked.isDone())
        {
            return null;
        }
        if (this.blocked.isCancelled())
        {
            this.close();
        }

        if (this.closed)
        {
            return null;
        }

        this.batchId++;
        VectorizedRowBatch rowBatch = null;
        int rowBatchSize = 0;

        Block[] blocks = new Block[this.numColumnToRead];

        if (this.numColumnToRead > 0)
        {
            try
            {
                do
                {
                    rowBatch = recordReader.readBatch(BatchSize, false);
                    if (rowBatch.size <= 0)
                    {
                        if (readNextPath())
                        {
                            return getNextPage();
                        } else
                        {
                            this.close();
                            return null;
                        }
                    }

                    if (this.filter.isPresent())
                    {
                        this.filter.get().doFilter(rowBatch, this.filtered, this.tmp);
                        rowBatch.applyFilter(this.filtered);
                    }
                    rowBatchSize = rowBatch.size;
                } while (rowBatchSize <= 0);

                for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
                {
                    Type type = columns.get(fieldId).getColumnType();
                    TypeDescription.Category typeCategory = columns.get(fieldId).getTypeCategory();
                    ColumnVector vector = rowBatch.cols[fieldId];
                    blocks[fieldId] = new LazyBlock(rowBatchSize, new PixelsBlockLoader(
                            this, vector, type, typeCategory, rowBatchSize));
                }
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new TrinoException(PixelsErrorCode.PIXELS_BAD_DATA, "read row batch error.", e);
            }
        } else
        {
            // No column to read.
            try
            {
                rowBatchSize = this.recordReader.prepareBatch(BatchSize);
                if (rowBatchSize <= 0)
                {
                    if (readNextPath())
                    {
                        return getNextPage();
                    } else
                    {
                        this.close();
                        return null;
                    }
                }
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new TrinoException(PixelsErrorCode.PIXELS_BAD_DATA, "prepare row batch error.", e);
            }
        }

        return new Page(rowBatchSize, blocks);
    }

    /**
     * Close the last reader.
     */
    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }

        closeReader();

        closed = true;
    }

    /**
     * Close the current pixels reader without closing this page source.
     */
    private void closeReader()
    {
        try
        {
            if (pixelsReader != null)
            {
                if (recordReader != null)
                {
                    this.completedBytes += recordReader.getCompletedBytes();
                    this.readTimeNanos += recordReader.getReadTimeNanos();
                    this.memoryUsage += recordReader.getMemoryUsage();
                }
                pixelsReader.close();
                /**
                 * PIXELS-114:
                 * Must set pixelsReader and recordReader to null,
                 * close() may be called multiple times by Presto.
                 */
                recordReader = null;
                pixelsReader = null;
            }
        } catch (Exception e)
        {
            logger.error("close error: " + e.getMessage());
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_CLOSE_ERROR, "close reader error.", e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try
        {
            close();
        } catch (RuntimeException e)
        {
            // Self-suppression not permitted
            logger.error(e, e.getMessage());
            if (throwable != e)
            {
                throwable.addSuppressed(e);
            }
            throw new TrinoException(PixelsErrorCode.PIXELS_CLIENT_ERROR, "close page source error.", e);
        }
    }

    @Override
    public int getBatchId()
    {
        return batchId;
    }
}
