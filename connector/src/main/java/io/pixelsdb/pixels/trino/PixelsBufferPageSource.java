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

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderBufferImpl;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.*;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.split.PixelsBufferSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class PixelsBufferPageSource implements PixelsPageSource
{
    private static final Logger logger = Logger.get(PixelsBufferPageSource.class);
    private static final Long pollIntervalMillis = 200L;
    private final PixelsBufferSplit split;
    private final List<PixelsColumnHandle> columns;
    private final PixelsTransactionHandle transactionHandle;
    private final String[] includeCols;
    private final CompletableFuture<?> blocked;
    private final int numColumnToRead;
    private final Optional<TableScanFilter> filter;
    private final Storage storage;
    private final RetinaService retinaService;
    private final String retinaServiceHost;
    private boolean closed;
    private long completedBytes = 0L;
    private final long memoryUsage = 0L;
    private int batchId;
    private List<Long> fileIds;
    private final int fileIdIndex = 0;
    private final boolean activeMemtableRead = false;
    private long startTimeNanos;
    private long totalReadTimeNanos;
    private PixelsReaderOption option;
    private PixelsRecordReaderBufferImpl reader;

    public PixelsBufferPageSource(PixelsBufferSplit split, List<PixelsColumnHandle> columnHandles,
                                  PixelsTransactionHandle transactionHandle,
                                  Storage storage)
    {
        this.startTimeNanos = System.nanoTime();
        this.split = split;
        this.transactionHandle = transactionHandle;
        this.columns = columnHandles;
        this.includeCols = new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            includeCols[i] = columns.get(i).getColumnName();
        }
        this.storage = storage;
        this.numColumnToRead = columnHandles.size();
        this.batchId = 0;
        this.closed = false;

        if(split.getAddresses().isEmpty())
        {
            this.retinaService = RetinaService.Instance();
            this.retinaServiceHost = ConfigFactory.Instance().getProperty("retina.server.host");
        } else
        {
            HostAddress retinaAddress = split.getAddresses().getFirst();
            this.retinaService = RetinaService.CreateInstance(retinaAddress.getHostText(), retinaAddress.getPort());
            this.retinaServiceHost = retinaAddress.getHostText();
        }

        TupleDomain<PixelsColumnHandle> tupleDomain = split.getConstraint();

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        if (split.getConstraint().getDomains().isPresent())
        {
            columnFilters = PixelsSplitManager.getColumnFilters(split.getSchemaName(), split.getTableName(),
                    includeCols, split.getConstraint());
        }

        if (transactionHandle.getTimestamp() >= 0L)
        {
            columnFilters.put(split.getOriginColumnSize(), getTimeStampColumnFilter(transactionHandle.getTimestamp()));
        }

        this.filter = Optional.of(new TableScanFilter(split.getSchemaName(), split.getTableName(), columnFilters));
        initWriterBuffer();
        this.blocked = NOT_BLOCKED;
    }

    /**
     * Create filter for Timestamp
     *
     * @param timeStamp
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ColumnFilter<Long> getTimeStampColumnFilter(long timeStamp)
    {
        Bound<Long> lowerBound = new Bound<Long>(Bound.Type.UNBOUNDED, -1L); // UNBOUNDED
        Bound<Long> upperBound = new Bound<Long>(Bound.Type.INCLUDED, timeStamp);
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
        return new ColumnFilter<>("hidden_column", columnType, filter);
    }

    private static String md5Hex(byte[] data)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest)
            {
                sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Get Data of writer buffer
     */
    private void initWriterBuffer()
    {
        try
        {
            this.option = new PixelsReaderOption();
            this.option.skipCorruptRecords(true);
            this.option.tolerantSchemaEvolution(true);
            this.option.enableEncodedColumnVector(true);
            this.option.readIntColumnAsIntVector(true);
            this.option.includeCols(includeCols);
            this.option.transId(split.getTransId());
            this.option.transTimestamp(transactionHandle.getTimestamp());
            TypeDescription schema = TypeDescription.fromString(split.getOriginSchemaString());

            RetinaProto.GetWriteBufferResponse response =
                    retinaService.getWriteBuffer(split.getSchemaName(), split.getTableName(), split.getvNodeId(), option.getTransTimestamp());
            byte[] activeMemtableData = response.getData().toByteArray();
            this.reader = new PixelsRecordReaderBufferImpl(
                    option,
                    retinaServiceHost,
                    activeMemtableData, response.getIdsList(),
                    response.getBitmapsList(),
                    storage,
                    split.getTableId(),
                    split.getvNodeId(),
                    schema
            );
        } catch (RetinaException | IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR,
                    "Can't get super version of: " + split.getSchemaName() + "/" + split.getTableName(), e);
        }
    }

    @Override
    public Page getNextPage()
    {
        startTimeNanos = System.nanoTime();
        try
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


            VectorizedRowBatch rowBatch = reader.readBatch();
            if (rowBatch.endOfFile)
            {
                this.close();
                return null;
            }
            this.batchId++;
            int rowBatchSize;
            Block[] blocks = new Block[this.numColumnToRead];
            int batchSize = rowBatch.size;
            if(batchSize == 0)
            {
                return new Page(0);
            }
            Bitmap filtered = new Bitmap(batchSize, true);
            Bitmap tmp = new Bitmap(batchSize, false);
            if (this.filter.isPresent())
            {
                this.filter.get().doFilter(rowBatch, filtered, tmp);
                rowBatch.applyFilter(filtered);
            }

            rowBatchSize = rowBatch.size;

            for (int i = 0; i < blocks.length; ++i)
            {
                Type type = columns.get(i).getColumnType();
                TypeDescription.Category typeCategory = columns.get(i).getTypeCategory();
                int fieldId = columns.get(i).getLogicalOrdinal();
                ColumnVector vector = rowBatch.cols[fieldId];
                blocks[i] = new LazyBlock(rowBatchSize, new PixelsBlockLoader(
                        this, vector, type, typeCategory, rowBatchSize));
            }

            completedBytes += reader.getCompletedBytes();
            return new Page(rowBatchSize, blocks);
        } catch (RuntimeException | IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_READER_ERROR, "Can't Read Retina Data", e);
        } finally
        {
            totalReadTimeNanos += System.nanoTime() - startTimeNanos;
        }

    }

    @Override
    public long getCompletedBytes()
    {
        return this.completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return totalReadTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return this.closed;
    }

    @Override
    public long getMemoryUsage()
    {
        return this.memoryUsage;
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

        closed = true;
    }

    @Override
    public int getBatchId()
    {
        return batchId;
    }

    public PixelsTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }
}
