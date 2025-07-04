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

import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.*;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.impl.PixelsTupleDomainPredicate;
import io.pixelsdb.pixels.trino.split.PixelsBufferSplit;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class PixelsBufferPageSource implements PixelsPageSource {

    private static final Logger logger = Logger.get(PixelsBufferPageSource.class);
    private static final Long pollIntervalMillis = 200L;
    private final int BatchSize;
    private final PixelsBufferSplit split;
    private final List<PixelsColumnHandle> columns;
    private final PixelsTransactionHandle transactionHandle;
    private final String[] includeCols;
    private boolean closed;
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
    private final Storage storage;

    private byte[] data;

    private boolean activeMemtableReaded = false;
    private final long startTimeNanos;
    private long totalReadTimeNanos;

    public PixelsBufferPageSource(PixelsBufferSplit split, List<PixelsColumnHandle> columnHandles,
            PixelsTransactionHandle transactionHandle,
            Storage storage) {
        this.startTimeNanos = System.nanoTime();
        this.split = split;
        this.transactionHandle = transactionHandle;
        this.columns = columnHandles;
        this.includeCols = new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i) {
            includeCols[i] = columns.get(i).getColumnName();
        }
        this.storage = storage;
        this.numColumnToRead = columnHandles.size();
        this.batchId = 0;
        this.closed = false;
        this.BatchSize = PixelsTrinoConfig.getBatchSize();
        this.filtered = new Bitmap(this.BatchSize, true);
        this.tmp = new Bitmap(this.BatchSize, false);
        TupleDomain<PixelsColumnHandle> tupleDomain = split.getConstraint();

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        if (split.getConstraint().getDomains().isPresent()) {
            columnFilters = PixelsSplitManager.getColumnFilters(split.getSchemaName(), split.getTableName(),
                    includeCols, split.getConstraint());
        }

        columnFilters.put(columns.size(), getTimeStampColumnFilter(transactionHandle.getTimestamp())); // TODO(AntiO2)
                                                                                                       // check if
                                                                                                       // columns.size()
                                                                                                       // is correct
        this.filter = Optional.of(new TableScanFilter(split.getSchemaName(), split.getTableName(), columnFilters));
        initWriterBuffer();
        this.blocked = NOT_BLOCKED;
    }

    /**
     * Get Data of writer buffer
     */
    private void initWriterBuffer() {
        if (split.isEmpty()) {
            this.close();
            return;
        }

        if (split.getConstraint().getDomains().isPresent() && !split.getColumnOrder().isEmpty()) {
            Map<PixelsColumnHandle, Domain> domains = split.getConstraint().getDomains().get();
            List<PixelsTupleDomainPredicate.ColumnReference<PixelsColumnHandle>> columnReferences = new ArrayList<>(
                    domains.size());
            for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet()) {
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
            // TODO(AntiO2): use predicate
        }
    }

    /**
     * 
     * @return true: fetch data to read; false: No more data to read
     */
    public boolean readNextData() {
        try {
            if (split.getRetinaSplitType() == PixelsBufferSplit.RetinaSplitType.ACTIVE_MEMTABLE) {
                if (activeMemtableReaded) {
                    // active memtable has been read. Finish this split
                    return false;
                }
                data = split.getActiveMemtableData();
                activeMemtableReaded = true;
                return true;
            }

            if (split.getRetinaSplitType() == PixelsBufferSplit.RetinaSplitType.FILE_ID) {
                Long id = split.getNextMemtableId();
                if (id == -1) {
                    return false;
                }
                String path = getMinioPathFromId(id);
                getMemtableDataFromMinio(path);
            }
            logger.error("pixelsReader error: storage handler is null");
            throw new IOException("pixelsReader error: storage handler is null.");
        } catch (IOException e) {
            logger.error("pixelsReader error: " + e.getMessage());
            // closeWithSuppression(e);
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR,
                    "create Pixels reader error.", e);
        }
    }

    @Override
    public Page getNextPage() {
        long start = System.nanoTime();
        try {
            if (!this.blocked.isDone()) {
                return null;
            }
            if (this.blocked.isCancelled()) {
                this.close();
            }

            if (this.closed) {
                return null;
            }
            if (!readNextData()) {
                this.close();
                return null;
            }
            this.batchId++;
            VectorizedRowBatch rowBatch = VectorizedRowBatch.deserialize(data);
            int rowBatchSize = 0;

            Block[] blocks = new Block[this.numColumnToRead];

            if (this.filter.isPresent()) {
                this.filter.get().doFilter(rowBatch, this.filtered, this.tmp);
                rowBatch.applyFilter(this.filtered);
            }

            rowBatchSize = rowBatch.size;

            for (int fieldId = 0; fieldId < blocks.length; ++fieldId) {
                Type type = columns.get(fieldId).getColumnType();
                TypeDescription.Category typeCategory = columns.get(fieldId).getTypeCategory();
                ColumnVector vector = rowBatch.cols[fieldId];
                blocks[fieldId] = new LazyBlock(rowBatchSize, new PixelsBlockLoader(
                        this, vector, type, typeCategory, rowBatchSize));
            }

            return new Page(rowBatchSize, blocks);
        } catch (RuntimeException e) {
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR, "Can't Read Retina Data", e);
        } finally {
            totalReadTimeNanos += System.nanoTime() - start;
        }

    }

    @Override
    public long getCompletedBytes() {
        if (closed) {
            return this.completedBytes;
        }
        return this.completedBytes + (data != null ? data.length : 0);
    }

    @Override
    public long getReadTimeNanos() {
        return totalReadTimeNanos;
    }

    @Override
    public boolean isFinished() {
        return this.closed;
    }

    @Override
    public long getMemoryUsage() {
        if (closed)
        {
            return memoryUsage;
        }
        return this.memoryUsage + (data != null ? data.length : 0);
    }

    /**
     * Close WriterBuffer Record Reader
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;
    }

    private String getMinioPathFromId(Long id) {
        String minioEntry = split.getSchemaName() + '/' + split.getTableName() + '/' + id;
        return minioEntry;
    }

    private void getMemtableDataFromMinio(String path) {
        // Firstly, if the id is an immutable memtable,
        // we need to wait for it to be flushed to the storage (currently implemented
        // using minio)
        boolean fileExists = false;
        try {
            while (!fileExists) {

                fileExists = storage.exists(path);
                if (fileExists) {
                    break;
                }
                Thread.sleep(pollIntervalMillis);
            }

            // Create Physical Reader & read this object fully
            PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path);
            Integer length = (int) reader.getFileLength();
            ByteBuffer buffer = reader.readFully(length);
            data = buffer.array();
        } catch (IOException e) {
            throw new TrinoException(PixelsErrorCode.PIXELS_RETINA_ERROR,
                    "Can't create the reader of retina storage.", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create filter for Timestamp
     * 
     * @param timeStamp
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ColumnFilter<Long> getTimeStampColumnFilter(long timeStamp) {
        Bound<Long> lowerBound = new Bound<Long>(Bound.Type.UNBOUNDED, -1L); // UNBOUNDED
        Bound<Long> upperBound = new Bound<Long>(Bound.Type.EXCLUDED, timeStamp);
        Range range = new Range(lowerBound, upperBound);
        TypeDescription.Category columnType = TypeDescription.Category.LONG;
        Class<?> filterJavaType = columnType.getInternalJavaType();
        Filter<Long> filter = new Filter<>(filterJavaType,
                new ArrayList(List.of(range)),
                new ArrayList<Bound<Long>>(),
                false,
                false,
                false,
                false);
        ColumnFilter<Long> columnFilter = new ColumnFilter<>("hidden_column", columnType, filter);
        return columnFilter;
    }

    @Override
    public int getBatchId() {
        return batchId;
    }

}
