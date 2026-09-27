/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.trino;

import com.google.protobuf.ByteString;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.rpc.IngestClient;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.ingest.IngestRows;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.trino.split.PixelsPrivateSplit;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOS_PER_MILLIS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/** Decodes only the exact, authorized private manifest carried by one owner split. */
public final class PixelsPrivatePageSource implements ConnectorPageSource
{
    private static final int INT128_LONG_WORD_COUNT = 2;
    private static final byte CANONICAL_FALSE = 0;
    private static final byte CANONICAL_TRUE = 1;

    private final PixelsPrivateSplit split;
    private final List<PixelsColumnHandle> columns;
    private final IngestClient client;
    private final IngestOptions options;
    private final TableSpec table;
    private final Type[] types;
    private final TypeDescription[] storageTypes;
    private final Map<String, Integer> ordinals = new HashMap<>();
    private final ArrayDeque<AppendRequest> batches = new ArrayDeque<>();
    private long nextBatchOffset;
    private boolean endOfInput;
    private boolean closed;
    private long completedBytes;
    private long readTimeNanos;
    private ByteString manifestDigest;

    public PixelsPrivatePageSource(
            PixelsPrivateSplit split,
            List<PixelsColumnHandle> columns,
            IngestClient client,
            IngestOptions options,
            TypeManager typeManager)
            throws IOException
    {
        this.split = split;
        this.columns = List.copyOf(columns);
        this.client = client;
        this.options = options;
        table = split.decodeTable();
        types = new Type[table.getColumnsCount()];
        storageTypes = new TypeDescription[table.getColumnsCount()];
        for (int ordinal = 0; ordinal < table.getColumnsCount(); ordinal++) {
            types[ordinal] = typeManager.fromSqlType(table.getColumns(ordinal).getType());
            storageTypes[ordinal] = IngestRows.supported(table.getColumns(ordinal).getType());
            ordinals.put(table.getColumns(ordinal).getName(), ordinal);
        }
        for (PixelsColumnHandle column : columns) {
            requireOrdinal(column);
        }
        if (split.getConstraint().getDomains().isPresent()) {
            for (PixelsColumnHandle column
                    : split.getConstraint().getDomains().get().keySet()) {
                requireOrdinal(column);
            }
        }
    }

    @Override
    public long getCompletedBytes() { return completedBytes; }

    @Override
    public long getReadTimeNanos() { return readTimeNanos; }

    @Override
    public boolean isFinished() { return closed || (endOfInput && batches.isEmpty()); }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (batches.isEmpty()) {
                fetch();
            }
            AppendRequest request = batches.pollFirst();
            if (request == null) {
                return null;
            }
            MutationBatch batch = IngestWire.decode(request);
            completedBytes = Math.addExact(completedBytes, batch.getPayloadBytes());
            List<byte[][]> rows = ColumnBatchCodec.decode(
                    batch.getPayload(),
                    batch.getRowCount(),
                    table.getColumnsCount(),
                    options.maxBatchRows,
                    options.maxBatchBytes);
            return page(rows);
        }
        catch (Exception e) {
            close();
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Cannot read transaction-private INSERT rows", e);
        }
        finally {
            readTimeNanos = Math.addExact(readTimeNanos, System.nanoTime() - start);
        }
    }

    private void fetch() throws IOException
    {
        PrivateReadPage page = client.participant(split.getOwner()).readPrivate(
                PrivateReadRequest.newBuilder()
                        .setTransactionId(split.getIngestTransactionId())
                        .setReaderStatementId(split.getStatementId())
                        .setTableId(split.getTableId())
                        .setReadOwnThroughOrdinal(split.getFrontier())
                        .setReadPinToken(split.getReadToken())
                        .setBatchOffset(nextBatchOffset)
                        .setMaxBatches(options.privateReadMaxBatches)
                        .setMaxBytes(options.privateReadMaxBytes)
                        .build());
        if (manifestDigest == null) {
            manifestDigest = page.getManifestDigest();
        }
        else if (!manifestDigest.equals(page.getManifestDigest())) {
            throw new IOException("Private manifest changed between pages");
        }
        if (page.getNextBatchOffset() < nextBatchOffset
                || (!page.getEndOfInput()
                        && page.getNextBatchOffset() == nextBatchOffset)) {
            throw new IOException("Private-read pagination did not advance");
        }
        batches.addAll(page.getBatchesList());
        nextBatchOffset = page.getNextBatchOffset();
        endOfInput = page.getEndOfInput();
        if (nextBatchOffset > split.getBatchCount()
                || (endOfInput && nextBatchOffset != split.getBatchCount())) {
            throw new IOException("Private-read page does not match the sealed batch count");
        }
        if (batches.isEmpty() && !endOfInput) {
            throw new IOException("Private-read page is empty before end of input");
        }
    }

    private Page page(List<byte[][]> rows) throws IOException
    {
        List<Object[]> nativeRows = new ArrayList<>(rows.size());
        for (byte[][] row : rows) {
            Object[] values = new Object[table.getColumnsCount()];
            for (int ordinal = 0; ordinal < values.length; ordinal++) {
                values[ordinal] = decode(
                        types[ordinal], storageTypes[ordinal], row[ordinal]);
            }
            if (matches(values)) {
                nativeRows.add(values);
            }
        }
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            PixelsColumnHandle column = columns.get(channel);
            int ordinal = requireOrdinal(column);
            Type type = types[ordinal];
            BlockBuilder builder = type.createBlockBuilder(null, nativeRows.size());
            for (Object[] row : nativeRows) {
                write(type, builder, row[ordinal]);
            }
            blocks[channel] = builder.build();
        }
        return new Page(nativeRows.size(), blocks);
    }

    private boolean matches(Object[] row) throws IOException
    {
        Optional<Map<PixelsColumnHandle, Domain>> domains =
                split.getConstraint().getDomains();
        if (domains.isEmpty()) {
            return true;
        }
        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (!entry.getValue().includesNullableValue(
                    row[requireOrdinal(entry.getKey())])) {
                return false;
            }
        }
        return true;
    }

    private int requireOrdinal(PixelsColumnHandle column) throws IOException
    {
        Integer ordinal = ordinals.get(column.getColumnName());
        if (ordinal == null || !types[ordinal].equals(column.getColumnType())) {
            throw new IOException(
                    "Private-read column does not match pinned schema: "
                            + column.getColumnName());
        }
        return ordinal;
    }

    private static Object decode(Type type, TypeDescription storage, byte[] value)
            throws IOException
    {
        if (value == null) {
            return null;
        }
        ByteBuffer input = ByteBuffer.wrap(value);
        Object decoded;
        switch (storage.getCategory()) {
            case BOOLEAN:
                requireWidth(value, Byte.BYTES, storage);
                if (value[0] != CANONICAL_FALSE && value[0] != CANONICAL_TRUE) {
                    throw new IOException("Invalid canonical boolean value");
                }
                decoded = value[0] == CANONICAL_TRUE;
                input.position(input.limit());
                break;
            case BYTE:
                requireWidth(value, Byte.BYTES, storage);
                decoded = (long) input.get();
                break;
            case SHORT:
                requireWidth(value, Short.BYTES, storage);
                decoded = (long) input.getShort();
                break;
            case INT:
            case DATE:
                requireWidth(value, Integer.BYTES, storage);
                decoded = (long) input.getInt();
                break;
            case LONG:
            case TIMESTAMP:
                requireWidth(value, Long.BYTES, storage);
                decoded = input.getLong();
                break;
            case TIME:
                requireWidth(value, Integer.BYTES, storage);
                decoded = Math.multiplyExact(
                        (long) input.getInt(), PICOS_PER_MILLIS);
                break;
            case FLOAT:
                requireWidth(value, Integer.BYTES, storage);
                decoded = Integer.toUnsignedLong(input.getInt());
                break;
            case DOUBLE:
                requireWidth(value, Double.BYTES, storage);
                decoded = input.getDouble();
                break;
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                requireWidth(
                        value,
                        decimal.isShort()
                                ? Long.BYTES
                                : Math.multiplyExact(Long.BYTES, INT128_LONG_WORD_COUNT),
                        storage);
                decoded = decimal.isShort()
                        ? input.getLong()
                        : Int128.valueOf(input.getLong(), input.getLong());
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            case VARBINARY:
                decoded = Slices.wrappedBuffer(value);
                input.position(input.limit());
                break;
            default:
                throw new IOException("Unsupported private-read type: " + storage);
        }
        if (input.hasRemaining()) {
            throw new IOException("Invalid canonical scalar width for " + storage);
        }
        return decoded;
    }

    private static void requireWidth(
            byte[] value, int expectedWidth, TypeDescription storage) throws IOException
    {
        if (value.length != expectedWidth) {
            throw new IOException("Invalid canonical scalar width for " + storage);
        }
    }

    private static void write(Type type, BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(builder, (Boolean) value);
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(builder, (Long) value);
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(builder, (Double) value);
        }
        else if (type.getJavaType() == io.airlift.slice.Slice.class) {
            type.writeSlice(builder, (io.airlift.slice.Slice) value);
        }
        else {
            type.writeObject(builder, value);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        long bytes = 0;
        for (AppendRequest batch : batches) {
            bytes = Math.addExact(bytes, batch.getSerializedSize());
        }
        return bytes;
    }

    @Override
    public void close()
    {
        closed = true;
        batches.clear();
    }
}
