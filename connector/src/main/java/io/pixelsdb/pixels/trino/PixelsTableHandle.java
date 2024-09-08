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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.logical.Table;
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

    private final Table.TableType tableType;
    private final PixelsJoinHandle joinHandle;
    private final PixelsAggrHandle aggrHandle;
    /**
     * The storage scheme of a base table.
     */
    private final Storage.Scheme storageScheme;
    /**
     * This is the storage paths of a base table to be observed by users.
     */
    private final List<String> storagePaths;

    /**
     * The constructor for bast table handle.
     * @param connectorId the connector id
     * @param schemaName the schema name
     * @param tableName the table name
     * @param columns the assignment columns (columns in the select statement) of the table
     * @param constraint the constraint (filter) on the table
     * @param tableType the type of the table (base for physical table)
     * @param joinHandle the handle of the join, must be non-null if tableType is JOINED
     * @param aggrHandle the handle of the aggregation, must be non-null if tableType is AGGREGATED
     */
    @JsonCreator
    public PixelsTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableAlias") String tableAlias,
            @JsonProperty("columns") List<PixelsColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint,
            @JsonProperty("tableType") Table.TableType tableType,
            @JsonProperty("joinHandle") PixelsJoinHandle joinHandle,
            @JsonProperty("aggrHandle") PixelsAggrHandle aggrHandle,
            @JsonProperty("storageScheme") Storage.Scheme storageScheme,
            @JsonProperty("storagePaths") List<String> storagePaths)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableAlias = requireNonNull(tableAlias, "tableAlias is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        if (this.tableType == Table.TableType.JOINED)
        {
            this.joinHandle = requireNonNull(joinHandle, "joinHandle is null");
        }
        else
        {
            this.joinHandle = null;
        }
        if (this.tableType == Table.TableType.AGGREGATED)
        {
            this.aggrHandle = requireNonNull(aggrHandle, "aggrHandle is null");
        }
        else
        {
            this.aggrHandle = null;
        }
        if (this.tableType == Table.TableType.BASE)
        {
            this.storageScheme = requireNonNull(storageScheme, "storageScheme is null");
            this.storagePaths = requireNonNull(storagePaths, "storagePaths is null");
        }
        else
        {
            this.storageScheme = null;
            this.storagePaths = null;
        }
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
    public Table.TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public PixelsJoinHandle getJoinHandle()
    {
        return joinHandle;
    }

    @JsonProperty
    public PixelsAggrHandle getAggrHandle()
    {
        return aggrHandle;
    }

    @JsonProperty
    public Storage.Scheme getStorageScheme()
    {
        return storageScheme;
    }

    @JsonProperty
    public List<String> getStoragePaths()
    {
        return storagePaths;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, tableAlias,
                columns, tableType, joinHandle, aggrHandle);
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
                Objects.equals(this.joinHandle, other.joinHandle) &&
                Objects.equals(this.aggrHandle, other.aggrHandle) &&
                Objects.equals(this.storageScheme, other.storageScheme) &&
                Objects.equals(this.storagePaths, other.getStoragePaths());
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaName == null ? "" : schemaName,
                tableName, tableAlias, tableType, aggrHandle == null ? "aggr=false" : "aggr=true",
                Joiner.on(",").join(columns));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Builder toBuilder()
    {
        return new Builder(this);
    }

    public static class Builder
    {
        private String builderConnectorId;
        private String builderSchemaName;
        private String builderTableName;
        private String builderTableAlias;
        private List<PixelsColumnHandle> builderColumns;
        private TupleDomain<PixelsColumnHandle> builderConstraint;
        private Table.TableType builderTableType;
        private PixelsJoinHandle builderJoinHandle;
        private PixelsAggrHandle builderAggrHandle;
        private Storage.Scheme builderStorageScheme;
        private List<String> builderStoragePaths;

        private Builder() { }

        private Builder(PixelsTableHandle tableHandle)
        {
            this.builderConnectorId = tableHandle.connectorId;
            this.builderSchemaName = tableHandle.schemaName;
            this.builderTableName = tableHandle.tableName;
            this.builderTableAlias = tableHandle.tableAlias;
            this.builderColumns = tableHandle.columns;
            this.builderConstraint = tableHandle.constraint;
            this.builderTableType = tableHandle.tableType;
            this.builderJoinHandle = tableHandle.joinHandle;
            this.builderAggrHandle = tableHandle.aggrHandle;
            this.builderStorageScheme = tableHandle.storageScheme;
            this.builderStoragePaths = tableHandle.storagePaths;
        }

        public void setConnectorId(String builderConnectorId)
        {
            this.builderConnectorId = builderConnectorId;
        }

        public void setSchemaName(String builderSchemaName)
        {
            this.builderSchemaName = builderSchemaName;
        }

        public void setTableName(String builderTableName)
        {
            this.builderTableName = builderTableName;
        }

        public void setTableAlias(String builderTableAlias)
        {
            this.builderTableAlias = builderTableAlias;
        }

        public void setColumns(List<PixelsColumnHandle> builderColumns)
        {
            this.builderColumns = builderColumns;
        }

        public void setConstraint(TupleDomain<PixelsColumnHandle> builderConstraint)
        {
            this.builderConstraint = builderConstraint;
        }

        public void setTableType(Table.TableType builderTableType)
        {
            this.builderTableType = builderTableType;
        }

        public void setJoinHandle(PixelsJoinHandle builderJoinHandle)
        {
            this.builderJoinHandle = builderJoinHandle;
        }

        public void setAggrHandle(PixelsAggrHandle builderAggrHandle)
        {
            this.builderAggrHandle = builderAggrHandle;
        }

        public void setStoragePaths(List<String> builderStoragePaths)
        {
            this.builderStoragePaths = builderStoragePaths;
        }


        public PixelsTableHandle build()
        {
            return new PixelsTableHandle(
                    builderConnectorId,
                    builderSchemaName,
                    builderTableName,
                    builderTableAlias,
                    builderColumns,
                    builderConstraint,
                    builderTableType,
                    builderJoinHandle,
                    builderAggrHandle,
                    builderStorageScheme,
                    builderStoragePaths
            );
        }
    }
}
