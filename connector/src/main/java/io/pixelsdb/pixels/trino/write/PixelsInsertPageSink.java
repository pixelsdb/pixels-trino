/*
 * Copyright 2026 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino.write;

import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.common.ingest.*;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.*;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.ingest.IngestRows;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static io.airlift.slice.SizeOf.sizeOf;

/** Worker-side bounded encoding and streaming. A sink seals streams; it never commits a transaction. */
public final class PixelsInsertPageSink implements ConnectorPageSink {
    private static final int BATCH_HEADER_BYTES = 3 * Integer.BYTES;
    private volatile PixelsMutationWriter writer;
    private final Supplier<PixelsMutationWriter> writerFactory;
    private final PixelsPageEncoder encoder;
    private final TableSpec table;
    private final TableIndex primary;
    private final int maxRows;
    private final int maxBytes;
    private final AtomicLong completedBytes = new AtomicLong();
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong pendingMemoryBytes = new AtomicLong();
    private byte[][][] pendingRows;
    private int pendingCount;
    private int pendingBytes = BATCH_HEADER_BYTES;
    private long batchCounter;
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Collection<Slice>> finishing;
    private volatile boolean aborted;

    public PixelsInsertPageSink(
            PixelsInsertTableHandle handle,
            long writerId,
            TypeManager types,
            MutationTransport transport,
            IngestOptions options)
            throws IOException {
        this(handle, () -> writerId, types, transport, options);
    }

    public PixelsInsertPageSink(
            PixelsInsertTableHandle handle,
            LongSupplier writerId,
            TypeManager types,
            MutationTransport transport,
            IngestOptions options)
            throws IOException {
        table = handle.decodeTable();
        primary = IngestRows.primary(table);
        encoder = new PixelsPageEncoder(table, handle.getInputColumns(), types);
        maxRows = options.maxBatchRows;
        maxBytes = options.maxBatchBytes;
        writerFactory = () -> {
            long allocatedWriterId = writerId.getAsLong();
            // Writer IDs are transaction-local. Include the transaction so single-row
            // keyless transactions do not all begin on the same vnode.
            batchCounter = Hashing.murmur3_128().newHasher()
                    .putLong(handle.getTransactionId()).putLong(allocatedWriterId).hash().asLong();
            return new PixelsMutationWriter(
                        handle.getTransactionId(),
                        handle.getStatementId(),
                        allocatedWriterId,
                        table.getTableId(),
                        table.getSchemaVersion(),
                        ColumnBatchCodec.FORMAT,
                        Math.multiplyExact((long) maxBytes, 2),
                        options.maxStreams,
                        transport);
        };
    }

    @Override
    public synchronized CompletableFuture<?> appendPage(Page page) {
        if (aborted || finishing != null) {
            throw new IllegalStateException("INSERT sink is not accepting pages");
        }
        if (!tail.isDone()) {
            throw new IllegalStateException("Caller must honor INSERT backpressure");
        }
        if (tail.isCompletedExceptionally()) {
            return tail;
        }
        if (page.getPositionCount() == 0) {
            return tail;
        }
        if (writer == null) {
            writer = writerFactory.get();
        }
        // RLE/dictionary pages can have a small retained size but a huge expanded size.
        // Encode incrementally, one bounded row window at a time, and await each window.
        retainedBytes.set(page.getRetainedSizeInBytes());
        tail = appendWindow(page, 0).whenComplete((ignored, error) -> {
            retainedBytes.set(0);
            if (error != null) releasePendingRows();
        });
        return tail;
    }

    private CompletableFuture<Void> appendWindow(Page page, int start) {
        if (start == page.getPositionCount()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            if (aborted) {
                throw new CancellationException("INSERT sink aborted");
            }
            if (primary == null) {
                return appendKeylessWindow(page, start);
            }
            Map<Integer, List<byte[][]>> groups = new LinkedHashMap<>();
            int remainingBytes = maxBytes - BATCH_HEADER_BYTES;
            int end = start;
            while (end < page.getPositionCount() && end - start < maxRows) {
                byte[][] row = encoder.encodeRow(page, end);
                long rowBytes = (long) Integer.BYTES * row.length;
                for (byte[] value : row) {
                    if (value != null) {
                        rowBytes += value.length;
                    }
                }
                if (rowBytes > maxBytes - BATCH_HEADER_BYTES) {
                    throw new IOException("One INSERT row exceeds the configured batch limit");
                }
                if (rowBytes > remainingBytes && end > start) {
                    break;
                }
                int shard = RetinaUtils.getBucketIdFromByteBuffer(IngestRows.indexKey(primary, row));
                IngestWire.route(table, shard);
                groups.computeIfAbsent(shard, ignored -> new ArrayList<>()).add(row);
                remainingBytes -= (int) rowBytes;
                end++;
            }
            CompletableFuture<Void> writes = CompletableFuture.completedFuture(null);
            for (Map.Entry<Integer, List<byte[][]>> group : groups.entrySet()) {
                byte[] payload =
                        ColumnBatchCodec.encode(
                                group.getValue(), table.getColumnsCount(), maxBytes);
                int count = group.getValue().size();
                int shard = group.getKey();
                writes =
                        writes.thenCompose(
                                        ignored ->
                                                writer.append(
                                                        MutationStreamId.Kind.APPEND_ROWS,
                                                        shard,
                                                        count,
                                                        payload))
                                .thenRun(() -> completedBytes.addAndGet(payload.length));
            }
            int next = end;
            // Avoid unbounded recursion when a local transport completes synchronously.
            return writes.thenComposeAsync(ignored -> appendWindow(page, next));
        } catch (Exception e) {
            CompletableFuture<Void> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }
    }

    private CompletableFuture<Void> appendKeylessWindow(Page page, int start) throws IOException {
        if (pendingRows == null) {
            pendingRows = new byte[maxRows][][];
            pendingMemoryBytes.set(sizeOf(pendingRows));
        }
        int end = start;
        while (end < page.getPositionCount() && pendingCount < maxRows) {
            byte[][] row = encoder.encodeRow(page, end);
            long rowBytes = (long) Integer.BYTES * row.length;
            long rowMemory = sizeOf(row);
            for (byte[] value : row) {
                if (value != null) {
                    rowBytes += value.length;
                    rowMemory += sizeOf(value);
                }
            }
            if (rowBytes > maxBytes - BATCH_HEADER_BYTES) {
                throw new IOException("One INSERT row exceeds the configured batch limit");
            }
            if (rowBytes > maxBytes - pendingBytes) break;
            pendingRows[pendingCount++] = row;
            pendingBytes += (int) rowBytes;
            pendingMemoryBytes.addAndGet(rowMemory);
            end++;
        }
        if (pendingCount == maxRows || pendingBytes == maxBytes || end < page.getPositionCount()) {
            int next = end;
            return flushPendingRows().thenComposeAsync(ignored -> appendWindow(page, next));
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> flushPendingRows() {
        if (aborted) return CompletableFuture.failedFuture(new CancellationException("INSERT sink aborted"));
        if (pendingCount == 0) return CompletableFuture.completedFuture(null);
        try {
            byte[] payload = ColumnBatchCodec.encode(
                    Arrays.asList(pendingRows).subList(0, pendingCount), table.getColumnsCount(), maxBytes);
            int count = pendingCount;
            int shard = table.getRoutes(Math.floorMod(batchCounter++, table.getRoutesCount())).getShardId();
            Arrays.fill(pendingRows, 0, pendingCount, null);
            pendingCount = 0;
            pendingBytes = BATCH_HEADER_BYTES;
            pendingMemoryBytes.set(sizeOf(pendingRows));
            return writer.append(MutationStreamId.Kind.APPEND_ROWS, shard, count, payload)
                    .thenRun(() -> completedBytes.addAndGet(payload.length));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void releasePendingRows() {
        pendingRows = null;
        pendingCount = 0;
        pendingBytes = BATCH_HEADER_BYTES;
        pendingMemoryBytes.set(0);
    }

    @Override
    public synchronized CompletableFuture<Collection<Slice>> finish() {
        if (finishing != null) {
            return finishing;
        }
        if (aborted) {
            throw new IllegalStateException("INSERT sink aborted");
        }
        finishing =
                tail.thenCompose(ignored -> flushPendingRows())
                        .thenCompose(ignored -> writer == null
                                ? CompletableFuture.<List<MutationStreamSeal>>completedFuture(Collections.emptyList())
                                : writer.finish())
                        .<Collection<Slice>>thenApply(
                                receipts -> {
                                    List<Slice> fragments = new ArrayList<>();
                                    for (MutationStreamSeal receipt : receipts) {
                                        fragments.add(
                                                Slices.wrappedBuffer(
                                                        IngestWire.encode(receipt).toByteArray()));
                                    }
                                    return fragments;
                                }).whenComplete((ignored, error) -> releasePendingRows());
        return finishing;
    }

    @Override
    public synchronized void abort() {
        aborted = true;
        if (writer != null) {
            writer.abort();
        }
        tail.whenComplete((ignored, error) -> releasePendingRows());
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes.get();
    }

    @Override
    public long getMemoryUsage() {
        return retainedBytes.get() + pendingMemoryBytes.get()
                + (writer == null ? 0 : Math.multiplyExact((long) maxBytes, 2));
    }
}
