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
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.MutationTransport;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/** Contract checks shared by the JUnit wrapper and the dependency-free runner. */
public final class MutationWriterContract
{
    private MutationWriterContract() {}

    public static void identitiesCountsAndKinds()
    {
        Fake transport = new Fake();
        PixelsMutationWriter writer = writer(100, 10, transport);
        append(writer, 0, 2, new byte[]{7, 7}).join();
        append(writer, 0, 2, new byte[]{7, 7}).join();
        append(writer, 1, 1, new byte[]{8}).join();
        writer.append(MutationStreamId.Kind.DELETE_ROWS, 0, 1, new byte[]{9}).join();
        List<MutationStreamSeal> seals = writer.finish().join();
        check(seals.size() == 3, "Kind/shard streams were conflated");
        check(seals.get(0).getRowCount() == 4 && seals.get(0).getBatchCount() == 2, "Equal user rows were deduplicated");
        check(transport.batches.get(1).getSequence() == 1, "Incorrect sequence");
        check(transport.batches.get(2).getSequence() == 0, "Shard sequence not independent");
        check(seals.get(2).getStreamId().getKind() == MutationStreamId.Kind.DELETE_ROWS, "Delete kind lost");
        expect(UnsupportedOperationException.class, () -> seals.add(seals.get(0)));
        check(writer.getBufferedBytes() == 0, "Payload budget leaked");
    }

    public static void finishWaitsForInflightAppend()
    {
        Fake transport = new Fake();
        CompletableFuture<Void> gate = new CompletableFuture<>();
        transport.responses.add(gate);
        PixelsMutationWriter writer = writer(100, 10, transport);
        CompletableFuture<Void> append = append(writer, 0, 1, new byte[]{1});
        CompletableFuture<List<MutationStreamSeal>> finish = writer.finish();
        check(finish == writer.finish(), "Repeated finish started new seals");
        check(!finish.isDone() && transport.seals.isEmpty(), "Sealed before append acknowledgement");
        expect(IllegalStateException.class, () -> append(writer, 0, 1, new byte[]{2}));
        gate.complete(null);
        append.join();
        check(finish.join().size() == 1 && transport.seals.size() == 1, "Incorrect seal count");
    }

    public static void requestOrder()
    {
        Fake transport = new Fake();
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        transport.responses.add(first); transport.responses.add(second);
        PixelsMutationWriter writer = writer(100, 10, transport);
        CompletableFuture<Void> a = append(writer, 0, 1, new byte[]{1});
        CompletableFuture<Void> b = append(writer, 0, 1, new byte[]{2});
        CompletableFuture<List<MutationStreamSeal>> finish = writer.finish();
        check(transport.batches.size() == 1, "Requests overtook each other");
        first.complete(null); a.join();
        check(transport.batches.size() == 2 && !finish.isDone(), "Second request not sequenced");
        second.complete(null); b.join(); finish.join();
    }

    public static void boundedAdmissionDoesNotConsumeSequence()
    {
        Fake transport = new Fake();
        CompletableFuture<Void> gate = new CompletableFuture<>(); transport.responses.add(gate);
        PixelsMutationWriter writer = writer(4, 10, transport);
        append(writer, 0, 1, new byte[]{1, 2, 3});
        expect(IllegalStateException.class, () -> append(writer, 0, 1, new byte[]{4, 5}));
        check(writer.getBufferedBytes() == 3, "Rejected batch retained memory");
        gate.complete(null);
        append(writer, 0, 1, new byte[]{4, 5}).join();
        check(transport.batches.get(1).getSequence() == 1, "Rejected batch consumed a sequence");
        writer.finish().join();
    }

    public static void appendFailurePreventsSeal()
    {
        Fake transport = new Fake();
        CompletableFuture<Void> gate = new CompletableFuture<>(); transport.responses.add(gate);
        PixelsMutationWriter writer = writer(100, 10, transport);
        CompletableFuture<Void> a = append(writer, 0, 1, new byte[]{1});
        CompletableFuture<Void> b = append(writer, 0, 1, new byte[]{2});
        CompletableFuture<List<MutationStreamSeal>> finish = writer.finish();
        gate.completeExceptionally(new IOException("lost response"));
        failed(IOException.class, a); failed(IOException.class, b); failed(IOException.class, finish);
        check(transport.batches.size() == 1 && transport.seals.isEmpty(), "Failure allowed further network calls");
        check(writer.getBufferedBytes() == 0, "Failed append leaked budget");
    }

    public static void synchronousTransportFailure()
    {
        Fake transport = new Fake(); transport.throwOnAppend = true;
        PixelsMutationWriter writer = writer(100, 10, transport);
        failed(IllegalStateException.class, append(writer, 0, 1, new byte[]{1}));
        failed(IllegalStateException.class, writer.finish());
        check(writer.getBufferedBytes() == 0, "Synchronous failure leaked budget");
    }

    public static void incorrectReceiptRejected()
    {
        Fake transport = new Fake(); transport.wrongSeal = true;
        PixelsMutationWriter writer = writer(100, 10, transport);
        append(writer, 0, 1, new byte[]{1}).join();
        failed(IllegalStateException.class, writer.finish());
    }

    public static void abortStopsQueuedCalls()
    {
        Fake transport = new Fake();
        CompletableFuture<Void> gate = new CompletableFuture<>(); transport.responses.add(gate);
        PixelsMutationWriter writer = writer(100, 10, transport);
        CompletableFuture<Void> a = append(writer, 0, 1, new byte[]{1});
        CompletableFuture<Void> b = append(writer, 0, 1, new byte[]{2});
        CompletableFuture<List<MutationStreamSeal>> finish = writer.finish();
        writer.abort(); writer.abort();
        failed(CancellationException.class, a); failed(CancellationException.class, b);
        failed(CancellationException.class, finish);
        gate.complete(null);
        check(transport.batches.size() == 1 && transport.seals.isEmpty(), "Late completion sent data or seals after abort");
        check(writer.getBufferedBytes() == 0, "Cancelled queue leaked budget after transport drained");
        expect(IllegalStateException.class, () -> append(writer, 0, 1, new byte[]{3}));
    }

    public static void emptyWriterDoesNotOpenStreams()
    {
        Fake transport = new Fake();
        PixelsMutationWriter writer = writer(100, 10, transport);
        check(writer.finish().join().isEmpty(), "Empty statement returned streams");
        check(transport.batches.isEmpty() && transport.seals.isEmpty(), "Empty statement performed I/O");
    }

    public static void payloadCopiesAndStreamLimit()
    {
        Fake transport = new Fake();
        PixelsMutationWriter writer = writer(100, 1, transport);
        byte[] bytes = {4};
        append(writer, 0, 1, bytes).join(); bytes[0] = 8;
        check(transport.batches.get(0).getPayload()[0] == 4, "Request retains mutable caller bytes");
        expect(IllegalStateException.class, () -> append(writer, 1, 1, new byte[]{2}));
        append(writer, 0, 1, new byte[]{2}).join();
        check(writer.finish().join().get(0).getBatchCount() == 2, "Rejected stream changed existing state");
    }

    public static void sealFailurePropagates()
    {
        Fake transport = new Fake(); transport.failSeal = true;
        PixelsMutationWriter writer = writer(100, 10, transport);
        append(writer, 0, 1, new byte[]{1}).join();
        failed(IOException.class, writer.finish());
    }

    public static void completionCanFinishReentrantly() throws Exception
    {
        Fake transport = new Fake();
        CompletableFuture<Void> gate = new CompletableFuture<>(); transport.responses.add(gate);
        PixelsMutationWriter writer = writer(100, 10, transport);
        CompletableFuture<Void> append = append(writer, 0, 1, new byte[]{1});
        CompletableFuture<Void> listener = append.thenRun(() -> writer.finish().join());
        CompletableFuture.runAsync(() -> gate.complete(null)).get(5, TimeUnit.SECONDS);
        listener.get(5, TimeUnit.SECONDS);
    }

    public static void invalidInputDoesNotOpenStream()
    {
        Fake transport = new Fake();
        PixelsMutationWriter writer = writer(100, 1, transport);
        expect(IllegalArgumentException.class, () -> append(writer, 0, 0, new byte[]{1}));
        expect(IllegalArgumentException.class, () -> append(writer, -1, 1, new byte[]{1}));
        expect(IllegalArgumentException.class, () -> append(writer, 0, 1, new byte[0]));
        check(writer.finish().join().isEmpty(), "Invalid batch created a stream");
    }

    public static void main(String[] args) throws Exception
    {
        identitiesCountsAndKinds(); finishWaitsForInflightAppend(); requestOrder();
        boundedAdmissionDoesNotConsumeSequence(); appendFailurePreventsSeal(); synchronousTransportFailure();
        incorrectReceiptRejected(); abortStopsQueuedCalls(); emptyWriterDoesNotOpenStreams();
        payloadCopiesAndStreamLimit(); sealFailurePropagates(); completionCanFinishReentrantly();
        invalidInputDoesNotOpenStream();
        System.out.println("MutationWriterContract: 13 cases passed");
    }

    private static PixelsMutationWriter writer(long budget, int streams, MutationTransport transport)
    {
        return new PixelsMutationWriter(1, 1, 2, 3, 7, 1, budget, streams, transport);
    }

    private static CompletableFuture<Void> append(PixelsMutationWriter writer, int shard, int rows, byte[] bytes)
    {
        return writer.append(MutationStreamId.Kind.APPEND_ROWS, shard, rows, bytes);
    }

    private static void check(boolean condition, String message)
    {
        if (!condition) { throw new AssertionError(message); }
    }

    private static void failed(Class<? extends Throwable> type, CompletableFuture<?> future)
    {
        try { future.join(); }
        catch (Throwable error)
        {
            while (error instanceof CompletionException && error.getCause() != null) { error = error.getCause(); }
            if (type.isInstance(error)) { return; }
            throw new AssertionError("Unexpected failure: " + error, error);
        }
        throw new AssertionError("Future should fail with " + type.getName());
    }

    private static void expect(Class<? extends Throwable> type, Runnable action)
    {
        try { action.run(); }
        catch (Throwable error)
        {
            if (type.isInstance(error)) { return; }
            throw new AssertionError("Unexpected error", error);
        }
        throw new AssertionError("Expected " + type.getName());
    }

    private static final class Fake implements MutationTransport
    {
        final List<MutationBatch> batches = new ArrayList<>();
        final List<MutationStreamSeal> seals = new ArrayList<>();
        final Deque<CompletableFuture<Void>> responses = new ArrayDeque<>();
        boolean throwOnAppend;
        boolean wrongSeal;
        boolean failSeal;

        @Override
        public CompletableFuture<Void> append(MutationBatch batch)
        {
            batches.add(batch);
            if (throwOnAppend) { throw new IllegalStateException("transport failed synchronously"); }
            return responses.isEmpty() ? CompletableFuture.completedFuture(null) : responses.removeFirst();
        }

        @Override
        public CompletableFuture<MutationStreamSeal> seal(MutationStreamSeal expected)
        {
            seals.add(expected);
            if (failSeal)
            {
                CompletableFuture<MutationStreamSeal> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IOException("sync failed"));
                return failed;
            }
            if (wrongSeal)
            {
                return CompletableFuture.completedFuture(new MutationStreamSeal(expected.getStreamId(),
                        expected.getBatchCount(), expected.getRowCount() + 1,
                        expected.getPayloadBytes(), expected.getDigest()));
            }
            return CompletableFuture.completedFuture(expected);
        }
    }
}
