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
package io.pixelsdb.pixels.trino.impl;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.pixelsdb.pixels.trino.PixelsTypeParser;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 */
public class PixelsMetadataProxy
{
    private static final Logger logger = Logger.get(PixelsMetadataProxy.class);
    private final MetadataService metadataService;
    private final PixelsTypeParser typeParser;
    private final MetadataCache metadataCache = MetadataCache.Instance();

    @Inject
    public PixelsMetadataProxy(PixelsTrinoConfig config, PixelsTypeParser typeParser)
    {
        requireNonNull(config, "config is null");
        this.typeParser = requireNonNull(typeParser, "typeParser is null");
        ConfigFactory configFactory = config.getConfigFactory();
        String host = configFactory.getProperty("metadata.server.host");
        int port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        this.metadataService = new MetadataService(host, port);
        Runtime.getRuntime().addShutdownHook(new Thread( () ->
        {
            try
            {
                this.metadataService.shutdown();
            } catch (InterruptedException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                        "Failed to shutdown metadata service (client).");
            }
        }));
    }

    public MetadataService getMetadataService()
    {
        return metadataService;
    }

    public List<String> getSchemaNames() throws MetadataException
    {
        List<String> schemaList = new ArrayList<String>();
        List<Schema> schemas = metadataService.getSchemas();
        for (Schema s : schemas) {
            schemaList.add(s.getName());
        }
        return schemaList;
    }

    public List<String> getTableNames(String schemaName) throws MetadataException
    {
        List<String> tableList = new ArrayList<String>();
        List<Table> tables = metadataService.getTables(schemaName);
        for (Table t : tables) {
            tableList.add(t.getName());
        }
        return tableList;
    }

    public List<String> getViewNames(String schemaName) throws MetadataException
    {
        List<String> viewList = new ArrayList<String>();
        List<View> views = metadataService.getViews(schemaName);
        for (View t : views) {
            viewList.add(t.getName());
        }
        return viewList;
    }

    public Table getTable(String schemaName, String tableName) throws MetadataException
    {
        return metadataService.getTable(schemaName, tableName);
    }

    public TypeDescription parsePixelsType(Type type)
    {
        return typeParser.parsePixelsType(type.getDisplayName());
    }

    public List<PixelsColumnHandle> getTableColumn(String connectorId, String schemaName, String tableName) throws MetadataException
    {
        ImmutableList.Builder<PixelsColumnHandle> columnsBuilder = ImmutableList.builder();
        List<Column> columnsList = metadataService.getColumns(schemaName, tableName, true);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        this.metadataCache.cacheTableColumns(schemaTableName, columnsList);
        for (int i = 0; i < columnsList.size(); i++) {
            Column c = columnsList.get(i);
            Type trinoType = typeParser.parseTrinoType(c.getType());
            TypeDescription pixelsType = typeParser.parsePixelsType(c.getType());
            if (trinoType == null || pixelsType == null)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                        "column type '" + c.getType() + "' is not supported.");
            }
            String name = c.getName();
            PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(
                    connectorId, schemaName, tableName, name, name, trinoType,
                    pixelsType.getCategory(), "", i);
            columnsBuilder.add(pixelsColumnHandle);
        }
        return columnsBuilder.build();
    }

    public List<Column> getColumnStatistics(String schemaName, String tableName)
    {
        return this.metadataCache.getTableColumns(new SchemaTableName(schemaName, tableName));
    }

    public List<Layout> getDataLayouts (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.getLayouts(schemaName, tableName);
    }

    public boolean createSchema (String schemaName) throws MetadataException
    {
        return metadataService.createSchema(schemaName);
    }

    public boolean dropSchema (String schemaName) throws MetadataException
    {
        return metadataService.dropSchema(schemaName);
    }

    public boolean createTable (String schemaName, String tableName, Storage.Scheme storageScheme,
                                List<String> basePathUris, List<Column> columns) throws MetadataException
    {
        return metadataService.createTable(schemaName, tableName, storageScheme, basePathUris, columns);
    }

    public boolean dropTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.dropTable(schemaName, tableName);
    }

    public boolean existTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.existTable(schemaName, tableName);
    }

    public boolean createView (String schemaName, String viewName, String viewData,
                               boolean updateIfExists) throws MetadataException
    {
        return metadataService.createView(schemaName, viewName, viewData, updateIfExists);
    }

    public boolean dropView (String schemaName, String viewName) throws MetadataException
    {
        return metadataService.dropView(schemaName, viewName);
    }

    public boolean existView (String schemaName, String viewName) throws MetadataException
    {
        return metadataService.existView(schemaName, viewName);
    }

    public View getView (String schemaName, String viewName, boolean returnNullIfNotExists) throws MetadataException
    {
        return metadataService.getView(schemaName, viewName, returnNullIfNotExists);
    }

    public List<View> getViews (String schemaName) throws MetadataException
    {
        return metadataService.getViews(schemaName);
    }

    public boolean existSchema (String schemaName) throws MetadataException
    {
        return metadataService.existSchema(schemaName);
    }
}