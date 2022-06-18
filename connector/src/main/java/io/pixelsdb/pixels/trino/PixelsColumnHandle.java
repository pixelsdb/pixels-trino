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
import io.pixelsdb.pixels.core.TypeDescription;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PixelsColumnHandle implements ColumnHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String columnAlias;
    private final Type columnType;
    private final TypeDescription.Category typeCategory;
    private final String columnComment;
    /**
     * The ordinal (index) in the columns of the table on which project and filter
     * push-down have not been applied. This is a logical ordinal, not the index in
     * the physical column order in the storage layout.
     * <p/>
     * <b>Note: </b> logical ordinal is not used in {@link #equals(Object) equals} and
     * {@link #hashCode() hashCode}.
     */
    private final int logicalOrdinal;

    @JsonCreator
    public PixelsColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnAlias") String columnAlias,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("typeCategory") TypeDescription.Category typeCategory,
            @JsonProperty("columnComment") String columnComment,
            @JsonProperty("logicalOrdinal") int logicalOrdinal)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(schemaName, "tableName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnAlias = requireNonNull(columnAlias, "columnAlias is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        this.columnComment = requireNonNull(columnComment, "columnComment is null");
        this.logicalOrdinal = logicalOrdinal;
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
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getColumnAlias()
    {
        return columnAlias;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public TypeDescription.Category getTypeCategory()
    {
        return typeCategory;
    }

    @JsonProperty
    public String getColumnComment() {
        return columnComment;
    }

    @JsonProperty
    public int getLogicalOrdinal()
    {
        return logicalOrdinal;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    /**
     * @return the synthetic column name that can be used in joins or aggregations.
     * It is composed of columnName_logicalOrdinal.
     */
    public String getSynthColumnName()
    {
        return this.columnName + "_" + this.logicalOrdinal;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, columnName, columnAlias, columnType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PixelsColumnHandle other = (PixelsColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnAlias, other.columnAlias) &&
                Objects.equals(this.columnType, other.columnType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("columnName", columnName)
                .add("columnAlias", columnAlias)
                .add("columnType", columnType)
                .add("columnComment", columnComment)
                .add("logicalOrdinal", logicalOrdinal)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(PixelsColumnHandle handle)
    {
        return new Builder(handle);
    }

    public static class Builder
    {
        private String builderConnectorId;
        private String builderSchemaName;
        private String builderTableName;
        private String builderColumnName;
        private String builderColumnAlias;
        private Type builderColumnType;
        private TypeDescription.Category builderTypeCategory;
        private String builderColumnComment;
        private int builderLogicalOrdinal;

        private Builder() {}

        private Builder(PixelsColumnHandle columnHandle)
        {
            this.builderConnectorId = columnHandle.connectorId;
            this.builderSchemaName = columnHandle.schemaName;
            this.builderTableName = columnHandle.tableName;
            this.builderColumnName = columnHandle.columnName;
            this.builderColumnAlias = columnHandle.columnAlias;
            this.builderColumnType = columnHandle.columnType;
            this.builderTypeCategory = columnHandle.typeCategory;
            this.builderColumnComment = columnHandle.columnComment;
            this.builderLogicalOrdinal = columnHandle.logicalOrdinal;
        }

        public Builder setConnectorId(String connectorId)
        {
            this.builderConnectorId = connectorId;
            return this;
        }

        public Builder setBuilderSchemaName(String builderSchemaName)
        {
            this.builderSchemaName = builderSchemaName;
            return this;
        }

        public Builder setBuilderTableName(String builderTableName)
        {
            this.builderTableName = builderTableName;
            return this;
        }

        public Builder setColumnName(String columnName)
        {
            this.builderColumnName = columnName;
            return this;
        }

        public Builder setColumnAlias(String columnAlias)
        {
            this.builderColumnAlias = columnAlias;
            return this;
        }

        public Builder setColumnType(Type columnType)
        {
            this.builderColumnType = columnType;
            return this;
        }

        public Builder setTypeCategory(TypeDescription.Category typeCategory)
        {
            this.builderTypeCategory = typeCategory;
            return this;
        }

        public Builder setColumnComment(String columnComment)
        {
            this.builderColumnComment = columnComment;
            return this;
        }

        public Builder setLogicalOrdinal(int logicalOrdinal)
        {
            this.builderLogicalOrdinal = logicalOrdinal;
            return this;
        }

        public PixelsColumnHandle build()
        {
            return new PixelsColumnHandle(
                    builderConnectorId,
                    builderSchemaName,
                    builderTableName,
                    builderColumnName,
                    builderColumnAlias,
                    builderColumnType,
                    builderTypeCategory,
                    builderColumnComment,
                    builderLogicalOrdinal
            );
        }
    }
}