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

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.pixelsdb.pixels.trino.block.TimeArrayBlock;
import io.pixelsdb.pixels.trino.block.VarcharArrayBlock;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.impl.PixelsTupleDomainPredicate;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.*;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @author guodong
 * @author tao
 */
class PixelsPageSource implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(PixelsPageSource.class);
    private final int BatchSize;
    private final PixelsSplit split;
    private final List<PixelsColumnHandle> columns;
    private final String[] includeCols;
    private final Storage storage;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private final PixelsCacheReader cacheReader;
    private final PixelsFooterCache footerCache;
    private final AtomicInteger localSplitCounter;
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

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles,
                            Storage storage, MemoryMappedFile cacheFile, MemoryMappedFile indexFile,
                            PixelsFooterCache pixelsFooterCache, CompletableFuture<?> lambdaOutput,
                            AtomicInteger localSplitCounter)
    {
        this.split = split;
        this.storage = storage;
        this.columns = columnHandles;
        this.includeCols =  new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            if (split.getTableType() == Table.TableType.BASE)
            {
                includeCols[i] = columns.get(i).getColumnName();
            }
            else
            {
                // Use the synthetic column name to access the join or aggregation result.
                includeCols[i] = columns.get(i).getSynthColumnName();
            }
        }
        this.numColumnToRead = columnHandles.size();
        this.footerCache = pixelsFooterCache;
        this.localSplitCounter = localSplitCounter;
        this.batchId = 0;
        this.closed = false;
        this.BatchSize = PixelsTrinoConfig.getBatchSize();
        this.filtered = new Bitmap(this.BatchSize, true);
        this.tmp = new Bitmap(this.BatchSize, false);

        this.cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();

        if (lambdaOutput == null)
        {
            if (split.getConstraint().getDomains().isPresent())
            {
                TableScanFilter scanFilter = PixelsSplitManager.createTableScanFilter(
                    split.getSchemaName(), split.getTableName(),
                    includeCols, split.getConstraint());
                this.filter = Optional.of(scanFilter);
            }
            else
            {
                this.filter = Optional.empty();
            }
            readFirstPath();
            this.blocked = NOT_BLOCKED;
        }
        else
        {
            /* If lambda (cloud function) is used, all filters have been pushed down,
             * no need to do filtering here.
             */
            this.filter = Optional.empty();
            this.blocked = lambdaOutput.whenComplete(((ret, err) -> {
                if (err != null)
                {
                    logger.error(err);
                    throw new RuntimeException(err);
                }
                try
                {
                    readFirstPath();
                }
                catch (Exception e)
                {
                    logger.error(e, "error in lambda output read.");
                    throw new RuntimeException(e);
                }
            }));
            if (this.blocked.isDone() && !this.blocked.isCancelled() &&
                    !this.blocked.isCompletedExceptionally() &&
                    !this.closed && this.recordReader == null)
            {
                // this.blocked is complete normally before reaching here.
                readFirstPath();
            }
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
        this.option.includeCols(includeCols);
        this.option.rgRange(split.getRgStart(), split.getRgLength());
        this.option.transId(split.getTransId());

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

    private boolean readNextPath ()
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
        if (this.blocked.isCancelled() || this.blocked.isCompletedExceptionally())
        {
            this.close();
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR,
                    "lambda request is done exceptionally: " +
                            this.blocked.isCompletedExceptionally());
        }

        if (this.closed)
        {
            return null;
        }

        this.batchId++;
        VectorizedRowBatch rowBatch;
        int rowBatchSize;

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
                            vector, type, typeCategory, rowBatchSize));
                }
                // update the current column in cached LSH index
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new TrinoException(PixelsErrorCode.PIXELS_BAD_DATA, "read row batch error.", e);
            }
        }
        else
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

    @Override
    public void close()
    {
        // PIXELS-403: page source is not accessed by multiple threads, there is no need to synchronize on this method.
        if (closed)
        {
            return;
        }

        closeReader();

        if (this.localSplitCounter != null)
        {
            this.localSplitCounter.decrementAndGet();
        }

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

    /**
     * Lazy Block Implementation for the Pixels
     */
    private final class PixelsBlockLoader
            implements LazyBlockLoader
    {
        private final int expectedBatchId = batchId;
        private final ColumnVector vector;
        private final Type type;
        private final TypeDescription.Category typeCategory;
        private final int batchSize;

        public PixelsBlockLoader(ColumnVector vector, Type type,
                                 TypeDescription.Category typeCategory, int batchSize)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.type = requireNonNull(type, "type is null");
            this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
            this.batchSize = batchSize;
        }

        @Override
        public Block load()
        {
            checkState(batchId == expectedBatchId);
            Block block;

            switch (typeCategory)
            {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                    LongColumnVector lcv = (LongColumnVector) vector;
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(lcv.isNull), lcv.vector);
                    break;
                case DOUBLE:
                case FLOAT:
                    /**
                     * According to TypeDescription.createColumn(),
                     * both float and double type use DoubleColumnVector, while they use
                     * FloatColumnReader and DoubleColumnReader respectively according to
                     * io.pixelsdb.pixels.reader.ColumnReader.newColumnReader().
                     */
                    DoubleColumnVector dbcv = (DoubleColumnVector) vector;
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(dbcv.isNull), dbcv.vector);
                    break;
                case DECIMAL:
                    /**
                     * PIXELS-196:
                     * Presto reads the unscaled values for decimal type here.
                     * The precision and scale of decimal are automatically processed by Presto.
                     */
                    if (vector instanceof DecimalColumnVector)
                    {
                        DecimalColumnVector dccv = (DecimalColumnVector) vector;
                        block = new LongArrayBlock(batchSize, Optional.ofNullable(dccv.isNull), dccv.vector);
                    }
                    else
                    {
                        LongDecimalColumnVector ldccv = (LongDecimalColumnVector) vector;
                        block = new Int128ArrayBlock(batchSize, Optional.ofNullable(ldccv.isNull), ldccv.vector);
                    }
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case BINARY:
                case VARBINARY:
                    if (vector instanceof BinaryColumnVector)
                    {
                        BinaryColumnVector scv = (BinaryColumnVector) vector;
                        block = new VarcharArrayBlock(batchSize, scv.vector, scv.start, scv.lens, scv.isNull);
                    }
                    else
                    {
                        DictionaryColumnVector dscv = (DictionaryColumnVector) vector;
                        Block dictionary = new VariableWidthBlock(dscv.dictOffsets.length - 1,
                                Slices.wrappedBuffer(dscv.dictArray), dscv.dictOffsets, Optional.empty());
                        if (!dscv.noNulls)
                        {
                            // Issue #84: Trino's stupid DictionaryBlock stores null value in dictionary.
                            int nullValueId = dictionary.getPositionCount();
                            dictionary = dictionary.copyWithAppendedNull();
                            for (int i = 0; i < batchSize; ++i)
                            {
                                if (dscv.isNull[i])
                                {
                                    dscv.ids[i] = nullValueId;
                                }
                            }
                        }
                        block = DictionaryBlock.create(batchSize, dictionary, dscv.ids);
                    }
                    break;
                case BOOLEAN:
                    ByteColumnVector bcv = (ByteColumnVector) vector;
                    block = new ByteArrayBlock(batchSize, Optional.ofNullable(bcv.isNull), bcv.vector);
                    break;
                case DATE:
                    // PIXELS-94: add date type.
                    DateColumnVector dtcv = (DateColumnVector) vector;
                    // In pixels and Presto, date is stored as the number of days from UTC 1970-1-1 0:0:0.
                    block = new IntArrayBlock(batchSize, Optional.ofNullable(dtcv.isNull), dtcv.dates);
                    break;
                case TIME:
                    // PIXELS-94: add time type.
                    TimeColumnVector tcv = (TimeColumnVector) vector;
                    /**
                     * In Presto, LongArrayBlock is used for time type. However, in Pixels,
                     * Time value is stored as int, so here we use TimeArrayBlock, which
                     * accepts int values but provides getLong method same as LongArrayBlock.
                     */
                    block = new TimeArrayBlock(batchSize, tcv.isNull, tcv.times);
                    break;
                case TIMESTAMP:
                    TimestampColumnVector tscv = (TimestampColumnVector) vector;
                    /**
                     * PIXELS-94: we have confirmed that LongArrayBlock is used for timestamp
                     * type in Presto.
                     *
                     * io.trino.spi.type.TimestampType extends
                     * io.trino.spi.type.AbstractLongType, which creates a LongArrayBlockBuilder.
                     * And this block builder builds a LongArrayBlock.
                     */
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(tscv.isNull), tscv.times);
                    break;
                case VECTOR:
                    VectorColumnVector vcv = (VectorColumnVector) vector;
                    // builder that simply concatenate all double arrays
                    BlockBuilder allDoublesBuilder = DOUBLE.createBlockBuilder(null, batchSize * vcv.dimension);
                    int[] offsets = new int[batchSize+1];
                    // build a block into which we put a double array
                    for (int i = 0 ; i < batchSize; i++) {
                        offsets[i] = i * vcv.dimension;
                        for (int j = 0; j < vcv.dimension; j++) {
                            DOUBLE.writeDouble(allDoublesBuilder, vcv.vector[i][j]);
                        }
                    }
                    offsets[batchSize] = batchSize * vcv.dimension;
                    // after extensive research on how other connectors deal with array type, the following seems to
                    // be the way to go: basically we stuff all the values of all arrays into one big block, and provide
                    // an int[] as offsets to tell trino where each array begins and ends. Note that the final offset
                    // should be the position to tell trino the end of the final array
                    // Interestingly all the above is NOT documented in trino documentation or code at all.
                    block = ArrayBlock.fromElementBlock(batchSize, Optional.of(vcv.isNull), offsets, allDoublesBuilder.build());
                    break;
                default:
                    BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize);
                    for (int i = 0; i < batchSize; ++i)
                    {
                        blockBuilder.appendNull();
                    }
                    block = blockBuilder.build();
                    break;
            }

            return block;
        }
    }
}