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

import com.alibaba.fastjson.JSON;
import com.google.inject.Inject;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.storage.MinIO;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.lambda.ScanInput;
import io.pixelsdb.pixels.core.lambda.ScanInvoker;
import io.pixelsdb.pixels.core.predicate.TableScanFilter;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider implements ConnectorPageSourceProvider
{
    // private static final Logger logger = Logger.get(PixelsPageSourceProvider.class);

    private final String connectorId;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final PixelsFooterCache pixelsFooterCache;
    private final PixelsTrinoConfig config;
    private final AtomicInteger localSplitCounter;

    @Inject
    public PixelsPageSourceProvider(PixelsConnectorId connectorId, PixelsTrinoConfig config)
            throws Exception
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(config, "config is null");
        if (config.getConfigFactory().getProperty("cache.enabled").equalsIgnoreCase("true"))
        {
            // NOTICE: creating a MemoryMappedFile is efficient, usually cost tens of us.
            this.cacheFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("cache.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("cache.size")));
            this.indexFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("index.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("index.size")));
        } else
        {
            this.cacheFile = null;
            this.indexFile = null;
        }
        this.pixelsFooterCache = new PixelsFooterCache();
        this.localSplitCounter = new AtomicInteger(0);
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split,
                                                ConnectorTableHandle table, List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter) {
        List<PixelsColumnHandle> pixelsColumns = columns.stream()
                .map(PixelsColumnHandle.class::cast).collect(toList());
        requireNonNull(split, "split is null");
        PixelsSplit pixelsSplit = (PixelsSplit) split;
        checkArgument(pixelsSplit.getConnectorId().equals(connectorId),
                "connectorId is not for this connector");

        String[] includeCols = new String[pixelsColumns.size()];
        for (int i = 0; i < pixelsColumns.size(); i++)
        {
            includeCols[i] = pixelsColumns.get(i).getColumnName();
        }

        try
        {
            if (config.isLambdaEnabled() && this.localSplitCounter.get() >= config.getLocalScanConcurrency())
            {
                MinIO.ConfigMinIO(config.getMinioEndpoint(), config.getMinioAccessKey(), config.getMinioSecretKey());
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaOutput(pixelsSplit, includeCols), null);
            }
            else
            {
                this.localSplitCounter.incrementAndGet();
                Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                        cacheFile, indexFile, pixelsFooterCache, null, this.localSplitCounter);
            }
        } catch (IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }
    }

    private CompletableFuture<?> getLambdaOutput(PixelsSplit inputSplit, String[] includeCols)
    {
        ScanInput scanInput = new ScanInput();
        scanInput.setQueryId(inputSplit.getQueryId());
        List<String> paths = inputSplit.getPaths();
        List<Integer> rgStarts = inputSplit.getRgStarts();
        List<Integer> rgLengths = inputSplit.getRgLengths();
        int splitSize = 0;
        ArrayList<ScanInput.InputInfo> inputInfos = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); ++i)
        {
            inputInfos.add(new ScanInput.InputInfo(paths.get(i), rgStarts.get(i), rgLengths.get(i)));
            splitSize += rgLengths.get(i);
        }
        scanInput.setInputs(inputInfos);
        scanInput.setCols(includeCols);
        scanInput.setSplitSize(splitSize);
        TableScanFilter filter = PixelsSplitManager.createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), includeCols, inputSplit.getConstraint());
        scanInput.setFilter(JSON.toJSONString(filter));
        String folder = config.getMinioOutputFolderForQuery(inputSplit.getQueryId());
        String endpoint = config.getMinioEndpoint();
        String accessKey = config.getMinioAccessKey();
        String secretKey = config.getMinioSecretKey();
        ScanInput.OutputInfo outputInfo = new ScanInput.OutputInfo(folder,
                endpoint, accessKey, secretKey, true);
        scanInput.setOutput(outputInfo);

        return ScanInvoker.invoke(scanInput).whenComplete(((scanOutput, err) -> {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(Storage.Scheme.minio, includeCols, scanOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }
}
