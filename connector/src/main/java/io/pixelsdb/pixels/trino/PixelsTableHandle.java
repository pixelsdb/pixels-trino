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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author tao
 * @author hank
 */
public final class PixelsTableHandle implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    /**
     * The assignments, i.e., the columns that appear in the select statement.
     * The columns that only appear in the constraint are not included.
     */
    private final List<PixelsColumnHandle> columns;
    private final TupleDomain<PixelsColumnHandle> constraint;

    public enum TableType
    {
        BASE, JOINED, AGGREGATED
    }

    private final TableType tableType;
    private final PixelsJoinHandle joinHandle;

    private final int nextSyntheticColumnId;

    /**
     * The constructor for bast table handle.
     * @param connectorId the connector id
     * @param schemaName the schema name
     * @param tableName the table name
     * @param columns the assignment columns (columns in the select statement) of the table
     * @param constraint the constraint (filter) on the table
     * @param tableType the type of the table (base for physical table)
     * @param joinHandle the information of the join
     */
    @JsonCreator
    public PixelsTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableAlias") String tableAlias,
            @JsonProperty("columns") List<PixelsColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("joinHandle") PixelsJoinHandle joinHandle,
            @JsonProperty("nextSyntheticColumnId") int nextSyntheticColumnId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableAlias = requireNonNull(tableName, "tableAlias is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        if (this.tableType == TableType.JOINED)
        {
            this.joinHandle = requireNonNull(joinHandle, "joinHandle is null");
        }
        else
        {
            this.joinHandle = null;
        }
        this.nextSyntheticColumnId = nextSyntheticColumnId;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTableAlias()
    {
        return tableAlias;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<PixelsColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public PixelsJoinHandle getJoinHandle()
    {
        return joinHandle;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public int getNextSyntheticColumnId()
    {
        return nextSyntheticColumnId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, tableAlias,
                columns, tableType, joinHandle);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass()))
        {
            return false;
        }

        PixelsTableHandle other = (PixelsTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tableAlias, other.tableAlias) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.tableType, other.tableType) &&
                Objects.equals(this.joinHandle, other.joinHandle);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaName == null ? "" : schemaName,
                tableName, tableAlias, tableType, Joiner.on(",").join(columns));
    }
}
