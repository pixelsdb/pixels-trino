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
package io.pixelsdb.pixels.trino.write;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.io.IOException;
import java.util.*;

/** Serializable pinned schema; handles contain no transport or mutable storage state. */
public final class PixelsInsertTableHandle implements ConnectorInsertTableHandle {
    private final long transactionId;
    private final long statementId;
    private final String table;
    private final List<PixelsColumnHandle> inputColumns;

    @JsonCreator
    public PixelsInsertTableHandle(
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("statementId") long statementId,
            @JsonProperty("table") String table,
            @JsonProperty("inputColumns") List<PixelsColumnHandle> inputColumns) {
        this.transactionId = transactionId;
        this.statementId = statementId;
        this.table = Objects.requireNonNull(table);
        this.inputColumns = List.copyOf(inputColumns);
    }

    @JsonProperty
    public long getTransactionId() {
        return transactionId;
    }

    @JsonProperty
    public long getStatementId() {
        return statementId;
    }

    @JsonProperty
    public String getTable() {
        return table;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getInputColumns() {
        return inputColumns;
    }

    public TableSpec decodeTable() throws IOException {
        return TableSpec.parseFrom(Base64.getDecoder().decode(table));
    }
}
