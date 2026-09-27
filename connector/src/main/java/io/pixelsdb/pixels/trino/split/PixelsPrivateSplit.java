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
package io.pixelsdb.pixels.trino.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** One owner-local, exact-manifest view of transaction-private WAL batches. */
public final class PixelsPrivateSplit extends PixelsSplit
{
    public static final String STORAGE_SCHEME = "transaction-private";

    private final long ingestTransactionId;
    private final long statementId;
    private final long tableId;
    private final long frontier;
    private final String owner;
    private final String readToken;
    private final String table;
    private final long batchCount;

    @JsonCreator
    public PixelsPrivateSplit(
            @JsonProperty("transId") long transId,
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint,
            @JsonProperty("ingestTransactionId") long ingestTransactionId,
            @JsonProperty("statementId") long statementId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("frontier") long frontier,
            @JsonProperty("owner") String owner,
            @JsonProperty("readToken") String readToken,
            @JsonProperty("table") String table,
            @JsonProperty("batchCount") long batchCount)
    {
        super(transId, splitId, connectorId, schemaName, tableName, STORAGE_SCHEME,
                addresses, List.of(), constraint);
        if (ingestTransactionId <= 0 || statementId <= 0 || tableId <= 0
                || frontier < 0 || batchCount <= 0) {
            throw new IllegalArgumentException("Invalid private split identity");
        }
        this.ingestTransactionId = ingestTransactionId;
        this.statementId = statementId;
        this.tableId = tableId;
        this.frontier = frontier;
        this.owner = Objects.requireNonNull(owner, "owner");
        this.readToken = Objects.requireNonNull(readToken, "readToken");
        this.table = Objects.requireNonNull(table, "table");
        this.batchCount = batchCount;
    }

    @JsonProperty
    public long getIngestTransactionId() { return ingestTransactionId; }

    @JsonProperty
    public long getStatementId() { return statementId; }

    @JsonProperty
    public long getTableId() { return tableId; }

    @JsonProperty
    public long getFrontier() { return frontier; }

    @JsonProperty
    public String getOwner() { return owner; }

    @JsonProperty
    public String getReadToken() { return readToken; }

    @JsonProperty
    public String getTable() { return table; }

    @JsonProperty
    public long getBatchCount() { return batchCount; }

    public TableSpec decodeTable() throws IOException
    {
        return TableSpec.parseFrom(Base64.getDecoder().decode(table));
    }
}
