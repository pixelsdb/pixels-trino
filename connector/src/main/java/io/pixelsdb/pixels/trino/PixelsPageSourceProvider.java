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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.trino.PixelsSplitManager.getIncludeColumns;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider implements ConnectorPageSourceProvider
{
    private static final Logger logger = Logger.get(PixelsPageSourceProvider.class);

    private final String connectorId;
    private final List<MemoryMappedFile> cacheFiles;
    private final List<MemoryMappedFile> indexFiles;
    private final PixelsFooterCache pixelsFooterCache;
    private final PixelsTrinoConfig config;

    @Inject
    public PixelsPageSourceProvider(PixelsConnectorId connectorId, PixelsTrinoConfig config)
            throws Exception
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(config, "config is null");
        if (config.getConfigFactory().getProperty("cache.enabled").equalsIgnoreCase("true"))
        {
            // NOTICE: creating a MemoryMappedFile is efficient, usually cost tens of us.
            int zoneNum = Integer.parseInt(config.getConfigFactory().getProperty("cache.zone.num"));
            int swapZoneNum = Integer.parseInt(config.getConfigFactory().getProperty("cache.zone.swap.num"));
            long zoneSize = Long.parseLong(config.getConfigFactory().getProperty("cache.size")) / (zoneNum - swapZoneNum);
            long zoneIndexSize = Long.parseLong(config.getConfigFactory().getProperty("index.size")) / (zoneNum - swapZoneNum);
            String zoneLocationPrefix = config.getConfigFactory().getProperty("cache.location");
            String indexLocationPrefix = config.getConfigFactory().getProperty("index.location");
            this.cacheFiles = new java.util.ArrayList<>();
            this.indexFiles = new java.util.ArrayList<>();
            for (int i = 0; i < zoneNum; i++) 
            {
                this.cacheFiles.add(new MemoryMappedFile(zoneLocationPrefix + "." + i, zoneSize));
                this.indexFiles.add(new MemoryMappedFile(indexLocationPrefix + "." + i, zoneIndexSize));
            }
            this.indexFiles.add(new MemoryMappedFile(indexLocationPrefix, zoneIndexSize));
        } else
        {
            this.cacheFiles = new java.util.ArrayList<>();
            this.indexFiles = new java.util.ArrayList<>();
        }
        this.pixelsFooterCache = new PixelsFooterCache();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split,
                                                ConnectorTableHandle table, List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter) {
        requireNonNull(table, "table is null");
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        requireNonNull(split, "split is null");
        PixelsSplit pixelsSplit = (PixelsSplit) split;
        checkArgument(pixelsSplit.getConnectorId().equals(connectorId),
                "connectorId is not for this connector");
        List<PixelsColumnHandle> pixelsColumns = columns.stream()
                .map(PixelsColumnHandle.class::cast).collect(toList());
        PixelsTransactionHandle pixelsTransactionHandle = (PixelsTransactionHandle) transactionHandle;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
            if (pixelsSplit.getFromServerlessOutput())
            {
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, pixelsTransactionHandle, storage,
                        cacheFiles, indexFiles, pixelsFooterCache);
            } else
            {
                // perform scan push down.
                List<PixelsColumnHandle> withFilterColumns = getIncludeColumns(pixelsColumns, tableHandle);
                return new PixelsPageSource(pixelsSplit, withFilterColumns, pixelsTransactionHandle, storage,
                        cacheFiles, indexFiles, pixelsFooterCache);
            }
        } catch (IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, e);
        }
    }
}
