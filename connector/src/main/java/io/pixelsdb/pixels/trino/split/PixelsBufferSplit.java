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

package io.pixelsdb.pixels.trino.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PixelsBufferSplit implements PixelsSplit {

    public enum RetinaSplitType {
        ACTIVE_MEMTABLE,
        FILE_ID // minio
    }

    private final long transId;
    private final long splitId;
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private List<Long> memtableIds;
    private byte[] activeMemtableData;
    private int index; // index for memtableIds
    private final List<HostAddress> addresses;
    private List<String> columnOrder;

    private final TupleDomain<PixelsColumnHandle> constraint;
    private final RetinaSplitType retinaSplitType;

    @JsonCreator
    public PixelsBufferSplit(
            @JsonProperty("transId") long transId,
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("memTableIds") List<Long> ids,
            @JsonProperty("activeMemtableData") byte[] activeMemtableData,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("columnOrder") List<String> columnOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint,
            @JsonProperty("retinaSplitType") RetinaSplitType type) {
        this.transId = transId;
        this.splitId = splitId;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.retinaSplitType = type;
        if (this.retinaSplitType == RetinaSplitType.ACTIVE_MEMTABLE) {
            this.activeMemtableData = activeMemtableData;
        } else {
            this.memtableIds = requireNonNull(ids, "Buffer Split type is FILE_ID, but ids is null");
        }
        this.index = 0;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.columnOrder = requireNonNull(columnOrder, "order is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    public Boolean isEmpty() {
        if (retinaSplitType == RetinaSplitType.ACTIVE_MEMTABLE) {
            return activeMemtableData == null || activeMemtableData.length == 0;
        } else {
            return memtableIds.isEmpty();
        }
    }

    @JsonProperty
    public long getTransId() {
        return transId;
    }

    @JsonProperty
    public long getSplitId() {
        return splitId;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public int getIndex() {
        return index;
    }

    public long getNextMemtableId() {
        if (index >= memtableIds.size()) {
            return -1;
        }
        return memtableIds.get(index++);
    }

    @JsonProperty
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @JsonProperty
    public List<String> getColumnOrder() {
        return columnOrder;
    }

    @JsonProperty
    public TupleDomain<PixelsColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty("retinaSplitType")
    public RetinaSplitType getRetinaSplitType() {
        return retinaSplitType;
    }

    @JsonProperty
    public List<Long> getMemtableIds() {
        return memtableIds;
    }

    @JsonProperty
    public byte[] getActiveMemtableData() {
        return activeMemtableData;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return 0L;
    }

    @Override
    public String getStorageScheme() {
        return "minio";
    }
}
