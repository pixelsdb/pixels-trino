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
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

public final class PixelsBufferSplit extends PixelsSplit
{
    private final int originColumnSize;
    private final String originSchemaString;

    @JsonCreator
    public PixelsBufferSplit(
            @JsonProperty("transId") long transId,
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("storageScheme") String storageScheme,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("columnOrder") List<String> columnOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint,
            @JsonProperty("originColumnSize") int originColumnSize,
            @JsonProperty("originSchemaString") String originSchemaString)
    {
        super(transId, splitId, connectorId, schemaName, tableName, storageScheme, addresses, columnOrder, constraint);
        this.originColumnSize = originColumnSize;
        this.originSchemaString = originSchemaString;
    }

    @JsonProperty
    public String getOriginSchemaString()
    {
        return originSchemaString;
    }

    @JsonProperty
    public int getOriginColumnSize()
    {
        return originColumnSize;
    }
}
