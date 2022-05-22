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
    private final String columnName;
    private final Type columnType;
    private final TypeDescription.Category typeCategory;
    private final String columnComment;
    private final int ordinalPosition;

    @JsonCreator
    public PixelsColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("typeCategory") TypeDescription.Category typeCategory,
            @JsonProperty("columnComment") String columnComment,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        this.columnComment = requireNonNull(columnComment, "columnComment is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
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
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
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
                Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("columnComment", columnComment)
                .add("ordinalPosition", ordinalPosition)
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
        private String builderColumnName;
        private Type builderColumnType;
        private TypeDescription.Category builderTypeCategory;
        private String builderColumnComment;
        private int builderOrdinalPosition;

        private Builder() {}

        private Builder(PixelsColumnHandle columnHandle)
        {
            this.builderConnectorId = columnHandle.connectorId;
            this.builderColumnName = columnHandle.columnName;
            this.builderColumnType = columnHandle.columnType;
            this.builderTypeCategory = columnHandle.typeCategory;
            this.builderColumnComment = columnHandle.columnComment;
            this.builderOrdinalPosition = columnHandle.ordinalPosition;
        }

        public Builder setBuilderConnectorId(String builderConnectorId)
        {
            this.builderConnectorId = builderConnectorId;
            return this;
        }

        public Builder setBuilderColumnName(String builderColumnName)
        {
            this.builderColumnName = builderColumnName;
            return this;
        }

        public Builder setBuilderColumnType(Type builderColumnType)
        {
            this.builderColumnType = builderColumnType;
            return this;
        }

        public Builder setBuilderTypeCategory(TypeDescription.Category builderTypeCategory)
        {
            this.builderTypeCategory = builderTypeCategory;
            return this;
        }

        public Builder setBuilderColumnComment(String builderColumnComment)
        {
            this.builderColumnComment = builderColumnComment;
            return this;
        }

        public Builder setBuilderOrdinalPosition(int builderOrdinalPosition)
        {
            this.builderOrdinalPosition = builderOrdinalPosition;
            return this;
        }

        public PixelsColumnHandle build()
        {
            return new PixelsColumnHandle(
                    builderConnectorId,
                    builderColumnName,
                    builderColumnType,
                    builderTypeCategory,
                    builderColumnComment,
                    builderOrdinalPosition
            );
        }
    }
}