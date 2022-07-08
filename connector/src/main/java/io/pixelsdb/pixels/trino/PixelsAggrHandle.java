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
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;

import java.util.List;

/**
 * @author hank
 * @date 04/07/2022
 */
public class PixelsAggrHandle
{
    private final List<PixelsColumnHandle> aggrColumns;
    private final List<PixelsColumnHandle> aggrResultColumns;
    private final List<PixelsColumnHandle> groupKeyColumns;
    private final List<PixelsColumnHandle> outputColumns;
    private final List<FunctionType> functionTypes;
    private final PixelsTableHandle originTable;

    @JsonCreator
    public PixelsAggrHandle(
            @JsonProperty("aggrColumns") List<PixelsColumnHandle> aggrColumns,
            @JsonProperty("aggrResultColumns") List<PixelsColumnHandle> aggrResultColumns,
            @JsonProperty("groupKeyColumns") List<PixelsColumnHandle> groupKeyColumns,
            @JsonProperty("outputColumns") List<PixelsColumnHandle> outputColumns,
            @JsonProperty("functionTypes") List<FunctionType> functionTypes,
            @JsonProperty("originTable") PixelsTableHandle originTable)
    {
        this.aggrColumns = aggrColumns;
        this.aggrResultColumns = aggrResultColumns;
        this.groupKeyColumns = groupKeyColumns;
        this.outputColumns = outputColumns;
        this.functionTypes = functionTypes;
        this.originTable = originTable;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getAggrColumns()
    {
        return aggrColumns;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getAggrResultColumns()
    {
        return aggrResultColumns;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getGroupKeyColumns()
    {
        return groupKeyColumns;
    }

    /**
     * Get the columns in the aggregation output, before further projections are applied
     * on the aggregation output. This is useful to recognize the group-key columns
     * of the aggregation.
     *
     * @return the columns in the aggregation output
     */
    @JsonProperty
    public List<PixelsColumnHandle> getOutputColumns()
    {
        return outputColumns;
    }

    @JsonProperty
    public List<FunctionType> getFunctionTypes()
    {
        return functionTypes;
    }

    @JsonProperty
    public PixelsTableHandle getOriginTable()
    {
        return originTable;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PixelsAggrHandle that = (PixelsAggrHandle) o;
        return Objects.equal(aggrColumns, that.aggrColumns) &&
                Objects.equal(aggrResultColumns, that.aggrResultColumns) &&
                Objects.equal(groupKeyColumns, that.groupKeyColumns) &&
                Objects.equal(outputColumns, that.outputColumns) &&
                Objects.equal(functionTypes, that.functionTypes) &&
                Objects.equal(originTable, that.originTable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(aggrColumns, aggrResultColumns, groupKeyColumns,
                outputColumns, functionTypes, originTable);
    }

    @Override
    public String toString()
    {
        return "PixelsAggrHandle{" +
                "aggrColumns=" + Joiner.on(",").join(aggrColumns) +
                ", aggrResultColumns=" + Joiner.on(",").join(aggrResultColumns) +
                ", groupKeyColumns=" + Joiner.on(",").join(groupKeyColumns) +
                ", outputColumns=" + Joiner.on(",").join(outputColumns) +
                ", functionTypes=" + Joiner.on(",").join(functionTypes) +
                ", originTable=" + originTable.toString() + '}';
    }
}
