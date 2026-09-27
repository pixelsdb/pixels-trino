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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Query-local writer for pre-encoded, pre-routed mutation batches. The Page
 * encoder and transaction coordinator are deliberately separate dependencies.
 *
 * <p>Requests are serialized; the returned append future is backpressure. A
 * caller must await outstanding appends before exceeding maxBufferedBytes.
 * The bound counts retained payload bytes, not the entire JVM object footprint.
 * finish() waits for accepted writes and validates durable stream receipts; it
 * never commits. abort() stops local submission but does not decide a remote
 * transaction outcome. The connector/coordinator must request authoritative
 * Abort separately, including on a lost transport response.
 *
 * <p>This writer is not registered as a ConnectorPageSink until participant
 * registration, Prepare/Commit, recovery, and visibility publication are wired.
 */
public final class PixelsMutationWriter
{
    private enum State { OPEN, FINISHING, FINISHED, FAILED, ABORTED }

    private final long transactionId;
    private final long statementId;
    private final long writerId;
    private final long tableId;
    private final long schemaVersion;
    private final int payloadFormat;
    private final long maxBufferedBytes;
    private final int maxStreams;
    private final MutationTransport transport;
    private final Map<MutationStreamId, Progress> streams = new LinkedHashMap<>();
    private final Set<CompletableFuture<Void>> appendResults = new HashSet<>();
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    private CompletableFuture<List<MutationStreamSeal>> finishResult;
    private State state = State.OPEN;
    private Throwable failure;
    private long bufferedBytes;

    public PixelsMutationWriter(long transactionId, long statementId, long writerId, long tableId,
                                long schemaVersion, int payloadFormat,
                                long maxBufferedBytes, int maxStreams,
                                MutationTransport transport)
    {
        if (transactionId <= 0 || statementId <= 0 || writerId <= 0 || tableId <= 0
                || schemaVersion < 0
                || payloadFormat <= 0 || maxBufferedBytes <= 0 || maxStreams <= 0)
        {
            throw new IllegalArgumentException("Invalid mutation writer configuration");
        }
        this.transactionId = transactionId;
        this.statementId = statementId;
        this.writerId = writerId;
        this.tableId = tableId;
        this.schemaVersion = schemaVersion;
        this.payloadFormat = payloadFormat;
        this.maxBufferedBytes = maxBufferedBytes;
        this.maxStreams = maxStreams;
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    public CompletableFuture<Void> append(MutationStreamId.Kind kind, int shardId,
                                          int rowCount, byte[] payload)
    {
        Objects.requireNonNull(payload, "payload");
        CompletableFuture<Void> start = new CompletableFuture<>();
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (this)
        {
            if (state != State.OPEN)
            {
                throw new IllegalStateException("Writer does not accept data in " + state, failure);
            }
            if (payload.length > maxBufferedBytes - bufferedBytes)
            {
                throw new IllegalStateException("Await outstanding appends before exceeding the payload budget");
            }
            MutationStreamId id = new MutationStreamId(
                    transactionId, statementId, writerId, tableId, shardId, kind);
            Progress progress = streams.get(id);
            if (progress == null && streams.size() >= maxStreams)
            {
                throw new IllegalStateException("Too many mutation streams");
            }
            long sequence = progress == null ? 0 : progress.batches;
            MutationBatch batch = new MutationBatch(id, sequence, schemaVersion,
                    payloadFormat, rowCount, payload);
            if (progress == null)
            {
                progress = new Progress();
                streams.put(id, progress);
            }
            progress.add(batch);
            bufferedBytes += batch.getPayloadBytes();
            appendResults.add(result);

            CompletableFuture<Void> operation = tail.thenCompose(ignored -> start)
                    .thenCompose(ignored -> sendAppend(batch));
            tail = operation.handle((ignored, error) -> {
                Throwable cause = unwrap(error);
                synchronized (PixelsMutationWriter.this)
                {
                    bufferedBytes -= batch.getPayloadBytes();
                    appendResults.remove(result);
                    if (cause != null) { recordFailure(cause); }
                }
                if (cause != null)
                {
                    throw new CompletionException(cause);
                }
                return null;
            });
            tail.whenComplete((ignored, error) -> {
                if (error == null) { result.complete(null); }
                else { result.completeExceptionally(unwrap(error)); }
            });
        }
        // Never invoke a transport while holding the submission monitor.
        start.complete(null);
        return result;
    }

    /** Idempotent local finish, including while the last append is in flight. */
    public CompletableFuture<List<MutationStreamSeal>> finish()
    {
        CompletableFuture<Void> start = new CompletableFuture<>();
        synchronized (this)
        {
            if (finishResult != null) { return finishResult; }
            finishResult = new CompletableFuture<>();
            if (state == State.ABORTED || state == State.FAILED)
            {
                finishResult.completeExceptionally(failure);
                return finishResult;
            }
            state = State.FINISHING;
            List<MutationStreamSeal> boundaries = new ArrayList<>();
            streams.forEach((id, progress) -> boundaries.add(progress.seal(id)));
            tail.thenCompose(ignored -> start).thenCompose(ignored -> sealAll(boundaries))
                    .whenComplete((receipts, error) -> {
                        Throwable cause = unwrap(error);
                        synchronized (PixelsMutationWriter.this)
                        {
                            if (state == State.ABORTED) { cause = failure; }
                            else if (cause != null) { recordFailure(cause); }
                            else { state = State.FINISHED; }
                        }
                        if (cause == null) { finishResult.complete(receipts); }
                        else { finishResult.completeExceptionally(cause); }
                    });
        }
        start.complete(null);
        return finishResult;
    }

    /** Advisory local cancellation. It is not a durable transaction Abort. */
    public void abort()
    {
        List<CompletableFuture<Void>> outstanding;
        CompletableFuture<List<MutationStreamSeal>> finishing;
        CancellationException cancelled;
        synchronized (this)
        {
            if (state == State.ABORTED || state == State.FINISHED) { return; }
            state = State.ABORTED;
            cancelled = new CancellationException("Mutation writer aborted locally");
            failure = cancelled;
            outstanding = new ArrayList<>(appendResults);
            finishing = finishResult;
        }
        outstanding.forEach(result -> result.completeExceptionally(cancelled));
        if (finishing != null) { finishing.completeExceptionally(cancelled); }
    }

    public synchronized long getBufferedBytes()
    {
        return bufferedBytes;
    }

    private CompletableFuture<Void> sendAppend(MutationBatch batch)
    {
        checkSending();
        return Objects.requireNonNull(transport.append(batch), "Transport returned a null append future");
    }

    private CompletableFuture<List<MutationStreamSeal>> sealAll(List<MutationStreamSeal> boundaries)
    {
        List<MutationStreamSeal> receipts = new ArrayList<>();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (MutationStreamSeal expected : boundaries)
        {
            chain = chain.thenCompose(ignored -> {
                checkSending();
                return Objects.requireNonNull(transport.seal(expected), "Transport returned a null seal future");
            }).thenAccept(receipt -> {
                if (!expected.equals(receipt))
                {
                    throw new IllegalStateException("Durable receipt differs from the submitted stream boundary");
                }
                receipts.add(receipt);
            });
        }
        return chain.thenApply(ignored -> Collections.unmodifiableList(new ArrayList<>(receipts)));
    }

    private synchronized void checkSending()
    {
        if (state == State.ABORTED || state == State.FAILED)
        {
            throw new CompletionException(failure);
        }
    }

    // Caller holds the monitor.
    private void recordFailure(Throwable cause)
    {
        if (state != State.ABORTED)
        {
            if (failure == null) { failure = cause; }
            state = State.FAILED;
        }
    }

    private static Throwable unwrap(Throwable error)
    {
        while (error instanceof CompletionException && error.getCause() != null)
        {
            error = error.getCause();
        }
        return error;
    }

    private static final class Progress
    {
        long batches;
        long rows;
        long bytes;
        byte[] digest = MutationStreamSeal.emptyDigest();

        void add(MutationBatch batch)
        {
            long nextRows = Math.addExact(rows, batch.getRowCount());
            long nextBytes = Math.addExact(bytes, batch.getPayloadBytes());
            long nextBatches = Math.addExact(batches, 1);
            digest = MutationStreamSeal.extendDigest(digest, batch.getDigest());
            rows = nextRows;
            bytes = nextBytes;
            batches = nextBatches;
        }

        MutationStreamSeal seal(MutationStreamId id)
        {
            return new MutationStreamSeal(id, batches, rows, bytes, digest);
        }
    }
}
