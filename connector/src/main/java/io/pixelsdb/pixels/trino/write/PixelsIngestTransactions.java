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

import static io.trino.spi.StandardErrorCode.*;

import com.google.inject.Inject;
import com.google.protobuf.Empty;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.pixelsdb.pixels.common.ingest.rpc.*;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.trino.*;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.*;
import java.util.concurrent.*;

import static io.trino.spi.type.BigintType.BIGINT;

/** Coordinator-local handles over server-owned transactions. Pin renewals are independent of SQL commit calls. */
public final class PixelsIngestTransactions implements Closeable {
    private static final Logger LOG = Logger.get(PixelsIngestTransactions.class);
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final int BYTES_PER_KIBIBYTE = 1024;
    private static final int MAX_WRITER_RECEIPT_KIBIBYTES = 64;
    private static final int MAX_WRITER_RECEIPT_BYTES =
            MAX_WRITER_RECEIPT_KIBIBYTES * BYTES_PER_KIBIBYTE;
    private static final long MINIMUM_RENEWAL_INTERVAL_MILLIS = 250L;
    private static final long RENEWALS_PER_LEASE = 4L;
    private static final long DEFAULT_VISIBILITY_BARRIER_TIMEOUT_SECONDS = 30L;
    private static final long MAX_VISIBILITY_BARRIER_TIMEOUT_SECONDS =
            TimeUnit.DAYS.toSeconds(1L);
    private static final long VISIBILITY_BARRIER_RPC_GRACE_SECONDS = 30L;
    private static final MethodHandle FLUSH_VISIBLE_BARRIER = methodHandle(
            "flushVisibleBarrier", long.class);

    private static final class ReadSession {
        final PixelsTransactionHandle handle;
        final Map<String, ReadPin> pins;
        final Map<String, ReadPin> privatePins = new ConcurrentHashMap<>();
        volatile Transaction write;
        volatile RuntimeException renewalFailure;
        PixelsInsertTableHandle openInsert;
        String openQueryId;
        long openStatementId;
        long completedOrdinal;

        ReadSession(PixelsTransactionHandle handle, Map<String, ReadPin> pins) {
            this.handle = handle;
            this.pins = pins;
        }
    }

    public record PrivateReadContext(
            long transactionId,
            long statementId,
            long tableId,
            long frontier,
            TableSpec table,
            Map<String, String> readTokens,
            Map<String, Long> ownerBatchCounts) {}

    private final boolean enabled;
    private final IngestOptions options;
    private final IngestClient client;
    private final TypeManager types;
    private final Map<Long, ReadSession> reads = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewals;

    @Inject
    public PixelsIngestTransactions(PixelsTrinoConfig config, TypeManager types) {
        this.enabled = config.isInsertEnabled();
        this.types = types;
        if (!enabled) {
            options = null;
            client = null;
            renewals = null;
            return;
        }
        try {
            options = new IngestOptions();
            if (!options.enabled
                    || !Boolean.parseBoolean(IngestOptions.property("retina.enable", "false"))
                    || !Boolean.parseBoolean(
                            IngestOptions.property("retina.buffer.split.enable", "true"))
                    || config.getCloudFunctionSwitch()
                            != PixelsTrinoConfig.CloudFunctionSwitch.OFF) {
                throw new IOException(
                        "INSERT requires transactional Retina, buffer splits, and"
                            + " cloud.function.switch=off");
            }
            client = IngestClient.fromConfig();
            renewals =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "pixels-ingest-leases");
                                t.setDaemon(true);
                                return t;
                            });
            long interval =
                    Math.max(
                            MINIMUM_RENEWAL_INTERVAL_MILLIS,
                            Math.min(options.readLeaseMillis, options.transactionLeaseMillis)
                                    / RENEWALS_PER_LEASE);
            renewals.scheduleWithFixedDelay(this::renew, interval, interval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new TrinoException(
                    CONFIGURATION_INVALID, "Cannot initialize transactional INSERT", e);
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public Set<Procedure> procedures() {
        if (!enabled) {
            return Collections.emptySet();
        }
        Procedure procedure = new Procedure(
                "system",
                "flush_visible_barrier",
                Collections.singletonList(new Argument(
                        "TIMEOUT_SECONDS",
                        BIGINT,
                        false,
                        DEFAULT_VISIBILITY_BARRIER_TIMEOUT_SECONDS)),
                FLUSH_VISIBLE_BARRIER.bindTo(this));
        return Collections.singleton(procedure);
    }

    public void flushVisibleBarrier(long timeoutSeconds) {
        requireEnabled();
        if (timeoutSeconds <= 0 || timeoutSeconds > MAX_VISIBILITY_BARRIER_TIMEOUT_SECONDS) {
            throw new TrinoException(
                    INVALID_PROCEDURE_ARGUMENT,
                    "timeout_seconds must be between 1 and "
                            + MAX_VISIBILITY_BARRIER_TIMEOUT_SECONDS);
        }
        try {
            client.coordinator(
                            Math.addExact(
                                    timeoutSeconds,
                                    VISIBILITY_BARRIER_RPC_GRACE_SECONDS),
                            TimeUnit.SECONDS)
                    .flushVisibleBarrier(
                    VisibleBarrierRequest.newBuilder()
                            .setDeadlineMillis(Math.addExact(
                                    System.currentTimeMillis(),
                                    TimeUnit.SECONDS.toMillis(timeoutSeconds)))
                            .build());
        }
        catch (Exception e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Retina visibility barrier did not complete",
                    e);
        }
    }

    private static MethodHandle methodHandle(String name, Class<?>... parameterTypes) {
        try {
            return MethodHandles.lookup().findVirtual(
                    PixelsIngestTransactions.class,
                    name,
                    MethodType.methodType(void.class, parameterTypes));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public IngestOptions options() {
        requireEnabled();
        return options;
    }

    public IngestClient client() {
        requireEnabled();
        return client;
    }

    private void requireEnabled() {
        if (!enabled) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Enable insert.enabled to use transactional Retina INSERT");
        }
    }

    public void beginRead(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        Map<String, ReadPin> pins = new LinkedHashMap<>();
        try {
            Publication publication =
                    client.coordinator().getPublication(Empty.getDefaultInstance());
            if (handle.getTimestamp() > publication.getPublishedTimestamp()) {
                throw new IOException("Unpublished read timestamp");
            }
            for (Route route : publication.getRoutesList()) {
                String owner = IngestWire.owner(route);
                if (!pins.containsKey(owner)) {
                    ReadPin request =
                            ReadPin.newBuilder()
                                    .setTransactionId(handle.getTransId())
                                    .setReadTimestamp(handle.getTimestamp())
                                    .build();
                    pins.put(owner, client.participant(owner).pinRead(request));
                }
            }
            if (pins.isEmpty()) {
                throw new IOException("No Retina owners are available");
            }
            Map<String, String> tokens = new HashMap<>();
            pins.forEach((owner, pin) -> tokens.put(owner, pin.getToken()));
            handle.setIngestReadTokens(tokens);
            reads.put(handle.getTransId(), new ReadSession(handle, pins));
        } catch (Exception e) {
            pins.forEach(
                    (owner, pin) -> {
                        try {
                            client.participant(owner).releaseRead(pin);
                        } catch (Exception ignored) {
                        }
                    });
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Cannot acquire a consistent Retina read view", e);
        }
    }

    private ReadSession session(PixelsTransactionHandle handle) {
        ReadSession read = reads.get(handle.getTransId());
        if (read == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Missing ingest read session");
        }
        if (read.renewalFailure != null) {
            throw read.renewalFailure;
        }
        return read;
    }

    public PixelsInsertTableHandle beginInsert(
            PixelsTransactionHandle handle,
            String queryId,
            PixelsTableHandle table,
            List<PixelsColumnHandle> columns) {
        requireEnabled();
        if (handle.isWriteProhibited()) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot INSERT in a read-only transaction");
        }
        ReadSession read = session(handle);
        synchronized (read) {
            if (read.openStatementId != 0 && !read.openQueryId.equals(queryId)) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Overlapping statements in one Pixels transaction are not supported");
            }
            if (read.openInsert != null) {
                if (read.openQueryId.equals(queryId)) {
                    return read.openInsert;
                }
            }
            try {
                long ordinal = read.openStatementId == 0
                        ? Math.addExact(read.completedOrdinal, FIRST_STATEMENT_ORDINAL)
                        : read.openStatementId;
                long statementId = ordinal;
                Transaction tx;
                if (read.write == null) {
                    tx = client.coordinator().beginWrite(
                            BeginWriteRequest.newBuilder()
                                    .setRequestId("trino:" + handle.getTransId())
                                    .setReadTimestamp(handle.getTimestamp())
                                    .setSchemaName(table.getSchemaName())
                                    .setTableName(table.getTableName())
                                    .setStatementId(statementId)
                                    .setQueryId(queryId)
                                    .setStatementOrdinal(ordinal)
                                    .setScope(handle.isAutoCommit()
                                            ? TransactionScope.AUTOCOMMIT
                                            : TransactionScope.EXPLICIT)
                                    .setRepresentation(options.writeRepresentation)
                                    .setAckMode(options.commitAckMode)
                                    .build());
                }
                else {
                    tx = client.coordinator().beginStatement(
                            BeginStatementRequest.newBuilder()
                                    .setTransactionId(read.write.getTransactionId())
                                    .setStatementId(statementId)
                                    .setQueryId(queryId)
                                    .setOrdinal(ordinal)
                                    .setReadOwnThroughOrdinal(read.completedOrdinal)
                                    .setSchemaName(table.getSchemaName())
                                    .setTableName(table.getTableName())
                                    .setWriteTable(true)
                                    .build());
                }
                read.write = tx;
                ensurePrivatePins(read);
                if (tx.getState() != TransactionState.OPEN) {
                    throw new IOException("Write transaction is no longer OPEN");
                }
                StatementManifest statement = tx.getStatementsList().stream()
                        .filter(value -> value.getStatementId() == statementId)
                        .findFirst()
                        .orElseThrow(() -> new IOException("Missing enlisted statement"));
                TableSpec descriptor = IngestWire.table(tx, statement.getTableId());
                new PixelsPageEncoder(
                        descriptor,
                        columns,
                        types); // validate supported types and input mapping before tasks start
                PixelsInsertTableHandle insert = new PixelsInsertTableHandle(
                        tx.getTransactionId(),
                        statementId,
                        Base64.getEncoder().encodeToString(descriptor.toByteArray()),
                        columns);
                read.openInsert = insert;
                read.openQueryId = queryId;
                read.openStatementId = statementId;
                return insert;
            } catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot begin Retina INSERT", e);
            }
        }
    }

    public void finishInsert(
            PixelsTransactionHandle handle,
            PixelsInsertTableHandle target,
            Collection<Slice> fragments) {
        ReadSession read = session(handle);
        if (read.write == null
                || read.openInsert == null
                || read.write.getTransactionId() != target.getTransactionId()
                || read.openInsert.getStatementId() != target.getStatementId()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "INSERT transaction identity mismatch");
        }
        try {
            if (fragments.size() > options.maxStreams) {
                throw new IOException("Too many writer fragments");
            }
            CompleteStatementRequest.Builder request = CompleteStatementRequest.newBuilder()
                    .setTransactionId(target.getTransactionId())
                    .setStatementId(target.getStatementId());
            for (Slice fragment : fragments) {
                if (fragment.length() > MAX_WRITER_RECEIPT_BYTES) {
                    throw new IOException("Oversized writer receipt");
                }
                request.addSeals(StreamSeal.parseFrom(fragment.getBytes()));
            }
            read.write = client.coordinator().completeStatement(request.build());
            StatementManifest completed = read.write.getStatementsList().stream()
                    .filter(value -> value.getStatementId() == target.getStatementId())
                    .findFirst()
                    .orElseThrow(() -> new IOException("Completed statement is absent"));
            if (completed.getState() != StatementState.STATEMENT_COMPLETE) {
                throw new IOException("Statement did not reach COMPLETE");
            }
            read.completedOrdinal = completed.getOrdinal();
            read.openInsert = null;
            read.openQueryId = null;
            read.openStatementId = 0;
        } catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Retina INSERT completion failed", e);
        }
    }

    public Optional<PrivateReadContext> enlistRead(
            PixelsTransactionHandle handle,
            String queryId,
            String schemaName,
            String tableName) {
        requireEnabled();
        ReadSession read = session(handle);
        synchronized (read) {
            if (read.write == null) {
                return Optional.empty();
            }
            if (read.openStatementId != 0 && !read.openQueryId.equals(queryId)) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Overlapping statements in one Pixels transaction are not supported");
            }
            try {
                long statementId = read.openStatementId == 0
                        ? Math.addExact(read.completedOrdinal, FIRST_STATEMENT_ORDINAL)
                        : read.openStatementId;
                Transaction tx = client.coordinator().beginStatement(
                        BeginStatementRequest.newBuilder()
                                .setTransactionId(read.write.getTransactionId())
                                .setStatementId(statementId)
                                .setQueryId(queryId)
                                .setOrdinal(statementId)
                                .setReadOwnThroughOrdinal(read.completedOrdinal)
                                .setSchemaName(schemaName)
                                .setTableName(tableName)
                                .setWriteTable(false)
                                .build());
                read.write = tx;
                ensurePrivatePins(read);
                read.openStatementId = statementId;
                read.openQueryId = queryId;
                StatementManifest statement = tx.getStatementsList().stream()
                        .filter(value -> value.getStatementId() == statementId)
                        .findFirst()
                        .orElseThrow(() -> new IOException("Missing private-read statement"));
                TableSpec descriptor = tx.getEnlistedTablesList().stream()
                        .filter(value -> value.getSchemaName().equals(schemaName)
                                && value.getTableName().equals(tableName))
                        .findFirst()
                        .orElseThrow(() -> new IOException("Missing private-read table"));
                Map<String, String> tokens = new HashMap<>();
                for (Route route : descriptor.getRoutesList()) {
                    String owner = IngestWire.owner(route);
                    ReadPin pin = read.privatePins.get(owner);
                    if (pin == null) {
                        throw new IOException("Missing read pin for Retina owner " + owner);
                    }
                    tokens.put(owner, pin.getToken());
                }
                Map<String, Long> ownerBatchCounts = new HashMap<>();
                for (StatementManifest completed : tx.getStatementsList()) {
                    if (completed.getState() != StatementState.STATEMENT_COMPLETE
                            || completed.getOrdinal()
                                    > statement.getReadOwnThroughOrdinal()
                            || completed.getTableId() != descriptor.getTableId()) {
                        continue;
                    }
                    for (StreamSeal seal : completed.getSealsList()) {
                        String owner = IngestWire.owner(IngestWire.route(
                                descriptor, seal.getStream().getShardId()));
                        ownerBatchCounts.merge(
                                owner, seal.getBatchCount(), Math::addExact);
                    }
                }
                return Optional.of(new PrivateReadContext(
                        tx.getTransactionId(),
                        statementId,
                        descriptor.getTableId(),
                        statement.getReadOwnThroughOrdinal(),
                        descriptor,
                        Map.copyOf(tokens),
                        Map.copyOf(ownerBatchCounts)));
            }
            catch (Exception e) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR, "Cannot enlist transaction-private read", e);
            }
        }
    }

    private void ensurePrivatePins(ReadSession read) throws IOException
    {
        if (read.write == null) {
            throw new IOException("Cannot pin a missing write transaction");
        }
        for (TableSpec table : read.write.getEnlistedTablesList()) {
            for (Route route : table.getRoutesList()) {
                String owner = IngestWire.owner(route);
                if (read.privatePins.containsKey(owner)) {
                    continue;
                }
                ReadPin request = ReadPin.newBuilder()
                        .setTransactionId(read.write.getTransactionId())
                        .setReadTimestamp(read.handle.getTimestamp())
                        .build();
                read.privatePins.put(owner, client.participant(owner).pinRead(request));
            }
        }
    }

    public void cleanupQuery(PixelsTransactionHandle handle, String queryId) {
        if (!enabled) {
            return;
        }
        ReadSession read = reads.get(handle.getTransId());
        if (read == null) {
            return;
        }
        synchronized (read) {
            if (read.openStatementId == 0 || !Objects.equals(read.openQueryId, queryId)) {
                return;
            }
            if (read.openInsert != null) {
                abort(handle);
                return;
            }
            try {
                read.write = client.coordinator().completeStatement(
                        CompleteStatementRequest.newBuilder()
                                .setTransactionId(read.write.getTransactionId())
                                .setStatementId(read.openStatementId)
                                .build());
                read.completedOrdinal = read.openStatementId;
                read.openStatementId = 0;
                read.openQueryId = null;
            }
            catch (Exception e) {
                abort(handle);
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Cannot complete transaction-private read statement",
                        e);
            }
        }
    }

    public void commit(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        ReadSession read = session(handle);
        if (read.write == null) {
            return;
        }
        if (read.openInsert != null) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Cannot commit an incomplete INSERT statement");
        }
        long id = read.write.getTransactionId();
        try {
            read.write = client.coordinator().prepareWrite(
                    PrepareWriteRequest.newBuilder().setTransactionId(id).build());
            Transaction outcome = client.coordinator().commitWrite(IngestWire.id(id));
            read.write = outcome;
            if (!acknowledged(outcome)) {
                throw new IOException("Commit has not reached the configured acknowledgement");
            }
        } catch (Exception e) {
            try {
                read.write = client.coordinator().getWrite(IngestWire.id(id));
                if (acknowledged(read.write)) {
                    return;
                }
            } catch (Exception statusFailure) {
                e.addSuppressed(statusFailure);
            }
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "INSERT outcome requires reconciliation for txId="
                            + id
                            + "; do not assume rollback or blindly resubmit the SQL",
                    e);
        }
    }

    private boolean acknowledged(Transaction outcome) {
        if (!IngestWire.committed(outcome) || outcome.getCommitToken().isEmpty()) {
            return false;
        }
        return outcome.getAckMode() == CommitAckMode.DURABLE
                || outcome.getState() == TransactionState.PUBLISHED;
    }

    public void abort(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        ReadSession read = reads.get(handle.getTransId());
        if (read == null || read.write == null) {
            return;
        }
        try {
            read.write =
                    client.coordinator().abortWrite(IngestWire.id(read.write.getTransactionId()));
            if (IngestWire.committed(read.write)) {
                LOG.warn(
                        "Cancellation arrived after COMMIT for ingest txId=%s",
                        read.write.getTransactionId());
            }
        } catch (Exception e) {
            LOG.warn(
                    e,
                    "Abort decision requires reconciliation for ingest txId=%s",
                    read.write.getTransactionId());
        }
    }

    public void endRead(PixelsTransactionHandle handle) {
        ReadSession read = reads.remove(handle.getTransId());
        if (read == null) {
            return;
        }
        read.pins.forEach(
                (owner, pin) -> {
                    try {
                        client.participant(owner).releaseRead(pin);
                    } catch (Exception e) {
                        LOG.debug(e, "Read pin release deferred to expiry");
                    }
                });
        read.privatePins.forEach(
                (owner, pin) -> {
                    try {
                        client.participant(owner).releaseRead(pin);
                    } catch (Exception e) {
                        LOG.debug(e, "Private read pin release deferred to expiry");
                    }
                });
    }

    private void renew() {
        for (ReadSession read : reads.values()) {
            try {
                if (read.renewalFailure != null) {
                    continue;
                }
                for (Map.Entry<String, ReadPin> entry : read.pins.entrySet()) {
                    client.participant(entry.getKey()).renewRead(entry.getValue());
                }
                for (Map.Entry<String, ReadPin> entry : read.privatePins.entrySet()) {
                    client.participant(entry.getKey()).renewRead(entry.getValue());
                }
                Transaction write = read.write;
                if (write != null
                        && !IngestWire.committed(write)
                        && write.getState() != TransactionState.ABORTED) {
                    client.coordinator().touchWrite(IngestWire.id(write.getTransactionId()));
                }
            } catch (Exception e) {
                read.renewalFailure =
                        new TrinoException(
                                GENERIC_INTERNAL_ERROR,
                                "Retina read/write lease could not be renewed",
                                e);
            }
        }
    }

    @Override
    public void close() {
        if (!enabled) {
            return;
        }
        renewals.shutdownNow();
        for (ReadSession read : new ArrayList<>(reads.values())) {
            abort(read.handle);
            endRead(read.handle);
        }
        client.close();
    }
}
