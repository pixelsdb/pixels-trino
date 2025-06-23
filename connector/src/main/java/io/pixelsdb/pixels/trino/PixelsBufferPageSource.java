/*
* Copyright 2025 PixelsDB.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.impl.PixelsTupleDomainPredicate;
import io.pixelsdb.pixels.trino.split.PixelsBufferSplit;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;

public class PixelsBufferPageSource implements ConnectorPageSource{

    private static final Logger logger = Logger.get(PixelsBufferPageSource.class);
    private final int BatchSize;
    private final PixelsBufferSplit split;
    private final List<PixelsColumnHandle> columns;
    private final PixelsTransactionHandle transactionHandle;
    private final String[] includeCols;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private final PixelsFooterCache footerCache;
    private final CompletableFuture<?> blocked;
    private final int numColumnToRead;
    private final Optional<TableScanFilter> filter;
    private final Bitmap filtered;
    private final Bitmap tmp;
    private long completedBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;
    private PixelsReaderOption option;
    private int batchId;

    public PixelsBufferPageSource(PixelsBufferSplit split, List<PixelsColumnHandle> columnHandles, PixelsTransactionHandle transactionHandle,
                            MemoryMappedFile cacheFile, MemoryMappedFile indexFile,
                            PixelsFooterCache pixelsFooterCache) {
        this.split = split;
        this.transactionHandle = transactionHandle;
        this.columns = columnHandles;
        this.includeCols =  new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            includeCols[i] = columns.get(i).getColumnName();
        }
        this.numColumnToRead = columnHandles.size();
        this.footerCache = pixelsFooterCache;
        this.batchId = 0;
        this.closed = false;
        this.BatchSize = PixelsTrinoConfig.getBatchSize();
        this.filtered = new Bitmap(this.BatchSize, true);
        this.tmp = new Bitmap(this.BatchSize, false);

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

        initWriterBuffer();
        this.blocked = NOT_BLOCKED;

    }

    private void initWriterBuffer() {
        // TODO get writerbuffer
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
            // closeWithSuppression(e);
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR,
                    "create Pixels reader error.", e);
        }
    }


    @Override
    public Page getNextPage() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getNextPage'");
    }

    @Override
    public long getCompletedBytes() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCompletedBytes'");
    }

    @Override
    public long getReadTimeNanos() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReadTimeNanos'");
    }

    @Override
    public boolean isFinished() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isFinished'");
    }

    @Override
    public long getMemoryUsage() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemoryUsage'");
    }

    /**
     * Close WriterBuffer Record Reader
     */
    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }

        // closeReader();

        closed = true;
    }

}
