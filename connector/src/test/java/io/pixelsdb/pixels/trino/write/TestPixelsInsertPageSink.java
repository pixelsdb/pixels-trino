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

import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.MutationTransport;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.ingest.IngestProto.Route;
import io.pixelsdb.pixels.ingest.IngestProto.TableColumn;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.trino.spi.Page;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.*;

public class TestPixelsInsertPageSink
{
    @Test
    public void emptySinksDoNotAllocateWriters() throws Exception
    {
        AtomicLong allocations = new AtomicLong();
        List<Long> streams = new ArrayList<>();
        PixelsInsertPageSink empty = sink(allocations, streams);
        empty.appendPage(new Page(0)).join();
        assertTrue(empty.finish().join().isEmpty());
        assertEquals(0, allocations.get());
        assertEquals(0, empty.getMemoryUsage());
        PixelsInsertPageSink aborted = sink(allocations, streams);
        aborted.abort();
        assertThrows(IllegalStateException.class, () -> aborted.appendPage(new Page(1)));
        assertEquals(0, allocations.get());
        assertTrue(streams.isEmpty());
    }

    @Test
    public void nonemptySinksAllocateIndependentWritersOnce() throws Exception
    {
        AtomicLong allocations = new AtomicLong();
        List<Long> streams = new ArrayList<>();
        PixelsInsertPageSink first = sink(allocations, streams);
        PixelsInsertPageSink second = sink(allocations, streams);
        first.appendPage(new Page(1)).join();
        first.appendPage(new Page(1)).join();
        second.appendPage(new Page(1)).join();
        assertEquals(2, allocations.get());
        assertTrue(streams.isEmpty(), "Partial batches remain bounded in the sink until finish");
        assertEquals(1, first.finish().join().size());
        assertEquals(1, second.finish().join().size());
        assertEquals(List.of(1L, 2L), streams);
        assertSame(first.finish(), first.finish());
    }

    private static PixelsInsertPageSink sink(AtomicLong allocations, List<Long> streams) throws Exception
    {
        return sink(1, 1, allocations, streams, new ArrayList<>());
    }

    @Test
    public void transactionLocalWriterIdsDoNotConcentrateKeylessRows() throws Exception
    {
        final int vnodeCount = 4;
        final int transactions = 32;
        List<Integer> shards = new ArrayList<>();
        for (long transaction = 1; transaction <= transactions; transaction++) {
            PixelsInsertPageSink sink = sink(transaction, vnodeCount,
                    new AtomicLong(), new ArrayList<>(), shards);
            sink.appendPage(new Page(1)).join();
            sink.finish().join();
        }
        assertEquals(vnodeCount, new HashSet<>(shards).size());
    }

    private static PixelsInsertPageSink sink(long transaction, int routes, AtomicLong allocations,
            List<Long> streams, List<Integer> shards) throws Exception
    {
        return sink(transaction, routes, allocations, streams, shards, new ArrayList<>(),
                CompletableFuture.completedFuture(null), new AtomicLong());
    }

    @Test
    public void combinesPagesWithBackpressureAndSealsOnlyAfterTail() throws Exception
    {
        List<MutationBatch> batches = new ArrayList<>();
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        AtomicLong seals = new AtomicLong();
        PixelsInsertPageSink sink = sink(1, 1, new AtomicLong(), new ArrayList<>(), new ArrayList<>(),
                batches, blocked, seals);
        int limit = new IngestOptions().maxBatchRows;
        sink.appendPage(new Page(limit - 1)).join();
        assertTrue(batches.isEmpty());
        CompletableFuture<?> append = sink.appendPage(new Page(2));
        assertFalse(append.isDone());
        assertEquals(limit, batches.getFirst().getRowCount());
        assertThrows(IllegalStateException.class, () -> sink.appendPage(new Page(1)));
        CompletableFuture<?> finish = sink.finish();
        assertFalse(finish.isDone());
        assertEquals(0, seals.get());
        blocked.complete(null);
        finish.join();
        assertEquals(List.of(limit, 1), batches.stream().map(MutationBatch::getRowCount).toList());
        assertEquals(1, seals.get());
        assertEquals(0L, batches.getFirst().getSequence());
        assertEquals(1L, batches.getLast().getSequence());
        for (MutationBatch batch : batches) {
            var rows = io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec.decode(
                    batch.getPayload(), batch.getRowCount(), 1, limit, new IngestOptions().maxBatchBytes);
            assertTrue(rows.stream().allMatch(row -> row[0] == null));
        }
    }

    @Test
    public void failedAppendCannotSealBufferedTail() throws Exception
    {
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        AtomicLong seals = new AtomicLong();
        List<MutationBatch> batches = new ArrayList<>();
        PixelsInsertPageSink sink = sink(1, 1, new AtomicLong(), new ArrayList<>(), new ArrayList<>(),
                batches, blocked, seals);
        sink.appendPage(new Page(new IngestOptions().maxBatchRows + 1));
        CompletableFuture<?> finish = sink.finish();
        blocked.completeExceptionally(new java.io.IOException("Injected append failure"));
        assertThrows(java.util.concurrent.CompletionException.class, finish::join);
        assertEquals(1, batches.size());
        assertEquals(0, seals.get());
    }

    @Test
    public void byteLimitFlushesBeforeRowLimitAcrossPages() throws Exception
    {
        String key = "retina.ingest.max.batch.bytes";
        var config = io.pixelsdb.pixels.common.utils.ConfigFactory.Instance();
        int original = new IngestOptions().maxBatchBytes;
        int rowsPerBatch = 3;
        int batchBytes = (3 + rowsPerBatch) * Integer.BYTES;
        try {
            config.addProperty(key, Integer.toString(batchBytes));
            List<MutationBatch> batches = new ArrayList<>();
            PixelsInsertPageSink sink = sink(1, 1, new AtomicLong(), new ArrayList<>(), new ArrayList<>(),
                    batches, CompletableFuture.completedFuture(null), new AtomicLong());
            sink.appendPage(new Page(rowsPerBatch - 1)).join();
            assertTrue(batches.isEmpty());
            sink.appendPage(new Page(rowsPerBatch)).join();
            sink.finish().join();
            assertEquals(List.of(rowsPerBatch, rowsPerBatch - 1),
                    batches.stream().map(MutationBatch::getRowCount).toList());
            assertTrue(batches.stream().allMatch(batch -> batch.getPayloadBytes() <= batchBytes));
        } finally {
            config.addProperty(key, Integer.toString(original));
        }
    }

    @Test
    public void abortDiscardsUnsentRowsAndReleasesTheirMemory() throws Exception
    {
        List<Long> streams = new ArrayList<>();
        PixelsInsertPageSink sink = sink(new AtomicLong(), streams);
        sink.appendPage(new Page(1)).join();
        long bufferedMemory = sink.getMemoryUsage();
        sink.abort();
        assertTrue(streams.isEmpty());
        assertTrue(sink.getMemoryUsage() < bufferedMemory);
        assertThrows(IllegalStateException.class, sink::finish);
    }

    private static PixelsInsertPageSink sink(long transaction, int routes, AtomicLong allocations,
            List<Long> streams, List<Integer> shards, List<MutationBatch> batches,
            CompletableFuture<Void> appendCompletion, AtomicLong seals) throws Exception
    {
        TableSpec.Builder table = TableSpec.newBuilder().setTableId(1).setSchemaVersion(1)
                .addColumns(TableColumn.newBuilder().setId(1).setName("id").setType("bigint"));
        for (int shard = 0; shard < routes; shard++) {
            table.addRoutes(Route.newBuilder().setShardId(shard));
        }
        PixelsInsertTableHandle handle = new PixelsInsertTableHandle(transaction, 1,
                Base64.getEncoder().encodeToString(table.build().toByteArray()), List.of());
        TypeManager types = (TypeManager) Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(), new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("fromSqlType")) {
                        return BIGINT;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        MutationTransport transport = new MutationTransport() {
            @Override
            public CompletableFuture<Void> append(MutationBatch batch) {
                batches.add(batch);
                streams.add(batch.getStreamId().getWriterId());
                shards.add(batch.getStreamId().getShardId());
                return appendCompletion;
            }

            @Override
            public CompletableFuture<MutationStreamSeal> seal(MutationStreamSeal expected) {
                seals.incrementAndGet();
                return CompletableFuture.completedFuture(expected);
            }
        };
        return new PixelsInsertPageSink(handle, allocations::incrementAndGet, types, transport, new IngestOptions());
    }
}
