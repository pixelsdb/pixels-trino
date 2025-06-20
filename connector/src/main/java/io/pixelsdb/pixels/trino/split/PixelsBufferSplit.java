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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkArgument;

public class PixelsBufferSplit implements PixelsSplit {


    private final long transId;
    private final long splitId;
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private String storageScheme;
    private List<String> paths;
    private List<Integer> rgStarts;
    private List<Integer> rgLengths;
    private int pathIndex;
    private final List<HostAddress> addresses;
    private List<String> columnOrder;
    private List<String> cacheOrder;
    private final TupleDomain<PixelsColumnHandle> constraint;

    @JsonCreator
    public PixelsBufferSplit(
            @JsonProperty("transId") long transId,
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("storageScheme") String storageScheme, // TODO: new storage scheme
            @JsonProperty("paths") List<String> paths,
            @JsonProperty("rgStarts") List<Integer> rgStarts,
            @JsonProperty("rgLengths") List<Integer> rgLengths,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("columnOrder") List<String> columnOrder,
            @JsonProperty("cacheOrder") List<String> cacheOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint) {
        this.transId = transId;
        this.splitId = splitId;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.storageScheme = requireNonNull(storageScheme, "storage scheme is null");
        this.paths = requireNonNull(paths, "paths is null");
        checkArgument(!paths.isEmpty(), "paths is empty");
        this.pathIndex = 0;
        this.rgStarts = requireNonNull(rgStarts, "rgStarts is null");
        checkArgument(rgStarts.size() == paths.size(),
                "the size of rgStarts and paths are different");
        this.rgLengths = requireNonNull(rgLengths, "rgLengths is null");
        checkArgument(rgLengths.size() == paths.size(),
                "the size of rgLengths and paths are different");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.columnOrder = requireNonNull(columnOrder, "order is null");
        this.cacheOrder = requireNonNull(cacheOrder, "cacheOrder is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    public Boolean isEmpty() {
        return paths.isEmpty();
    }

    public String getPath()
    {
        return this.paths.get(this.pathIndex);
    }

    public int getRgStart()
    {
        return this.rgStarts.get(pathIndex);
    }

    public int getRgLength()
    {
        return this.rgLengths.get(pathIndex);
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
    public String getStorageScheme() {
        return storageScheme;
    }

    @JsonProperty
    public List<String> getPaths() {
        return paths;
    }

    @JsonProperty
    public List<Integer> getRgStarts() {
        return rgStarts;
    }

    @JsonProperty
    public List<Integer> getRgLengths() {
        return rgLengths;
    }

    @JsonProperty
    public int getPathIndex() {
        return pathIndex;
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
    public List<String> getCacheOrder() {
        return cacheOrder;
    }

    @JsonProperty
    public TupleDomain<PixelsColumnHandle> getConstraint() {
        return constraint;
    }
}
