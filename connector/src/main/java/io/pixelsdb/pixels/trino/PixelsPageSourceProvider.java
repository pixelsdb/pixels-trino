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
import io.airlift.log.Logger;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.storage.MinIO;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.lambda.InvokerFactory;
import io.pixelsdb.pixels.executor.lambda.WorkerType;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.*;
import io.pixelsdb.pixels.executor.lambda.output.AggregationOutput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import io.pixelsdb.pixels.executor.lambda.output.Output;
import io.pixelsdb.pixels.executor.lambda.output.ScanOutput;
import io.pixelsdb.pixels.executor.plan.Table;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.trino.PixelsSplitManager.*;
import static java.util.Objects.requireNonNull;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider implements ConnectorPageSourceProvider
{
    private static final Logger logger = Logger.get(PixelsPageSourceProvider.class);

    private final String connectorId;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final PixelsFooterCache pixelsFooterCache;
    private final PixelsTrinoConfig config;
    private final AtomicInteger localSplitCounter;
    private final boolean computeFinalAggrInServer;

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
        this.computeFinalAggrInServer = Boolean.parseBoolean(
                config.getConfigFactory().getProperty("aggregation.compute.final.in.server"));
        this.pixelsFooterCache = new PixelsFooterCache();
        this.localSplitCounter = new AtomicInteger(0);
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
        String[] includeCols = pixelsSplit.getIncludeCols();
        List<PixelsColumnHandle> pixelsColumns = tableHandle.getColumns();

        try
        {
            if (pixelsSplit.getTableType() == Table.TableType.AGGREGATED)
            {
                // perform aggregation push down.
                MinIO.ConfigMinIO(config.getMinioEndpoint(), config.getMinioAccessKey(), config.getMinioSecretKey());
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaAggrOutput(pixelsSplit), null);
            }
            if (pixelsSplit.getTableType() == Table.TableType.JOINED)
            {
                // perform join push down.
                MinIO.ConfigMinIO(config.getMinioEndpoint(), config.getMinioAccessKey(), config.getMinioSecretKey());
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaJoinOutput(pixelsSplit), null);
            }
            if (pixelsSplit.getTableType() == Table.TableType.BASE)
            {
                // perform scan push down.
                pixelsColumns = getIncludeColumns(tableHandle);
                if (config.isLambdaEnabled() && this.localSplitCounter.get() >= config.getLocalScanConcurrency())
                {
                    MinIO.ConfigMinIO(config.getMinioEndpoint(), config.getMinioAccessKey(), config.getMinioSecretKey());
                    Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                    IntermediateFileCleaner.Instance().registerStorage(storage);
                    return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFile, indexFile,
                            pixelsFooterCache, getLambdaScanOutput(pixelsSplit), null);
                } else
                {
                    this.localSplitCounter.incrementAndGet();
                    Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                    return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                            cacheFile, indexFile, pixelsFooterCache, null, this.localSplitCounter);
                }
            }
            throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, new UnsupportedOperationException(
                    "table type '" + pixelsSplit.getTableType() + "' is not supported for building page source"));
        } catch (IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, e);
        }
    }

    private CompletableFuture<?> getLambdaAggrOutput(PixelsSplit inputSplit)
    {
        String aggrInputJson = inputSplit.getAggrInput();
        AggregationInput aggrInput = JSON.parseObject(aggrInputJson, AggregationInput.class);
        CompletableFuture<Output> aggrOutputFuture;

        if (computeFinalAggrInServer)
        {
            // TODO: execute the final aggregation locally.
            aggrOutputFuture = null;
        }
        else
        {
            aggrOutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.AGGREGATION).invoke(aggrInput);
        }

        return aggrOutputFuture.whenComplete(((aggrOutput, err) ->
        {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(Storage.Scheme.minio, inputSplit.getIncludeCols(), (AggregationOutput) aggrOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }

    private CompletableFuture<?> getLambdaJoinOutput(PixelsSplit inputSplit)
    {
        String joinInputJson = inputSplit.getJoinInput();
        JoinInput joinInput;
        if (inputSplit.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN)
        {
            joinInput = JSON.parseObject(joinInputJson, BroadcastChainJoinInput.class);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.BROADCAST)
        {
            joinInput = JSON.parseObject(joinInputJson, BroadcastJoinInput.class);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.PARTITIONED_CHAIN)
        {
            joinInput = JSON.parseObject(joinInputJson, PartitionedChainJoinInput.class);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            joinInput = JSON.parseObject(joinInputJson, PartitionedJoinInput.class);
        }
        else
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, "join algorithm '" +
                    inputSplit.getJoinAlgo() + "' is not supported");
        }
        MultiOutputInfo output = joinInput.getOutput();
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, config.getMinioEndpoint(),
                config.getMinioAccessKey(), config.getMinioSecretKey());
        output.setStorageInfo(storageInfo);
        output.setPath(config.getMinioOutputFolderForQuery(inputSplit.getQueryId(),
                inputSplit.getSchemaName() + "_" + inputSplit.getTableName()));
        // logger.info("join input: " + JSON.toJSONString(joinInput));
        CompletableFuture<Output> joinOutputFuture;
        if (inputSplit.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN)
        {
            joinOutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(joinInput);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.BROADCAST)
        {
            joinOutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.BROADCAST_JOIN).invoke(joinInput);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.PARTITIONED_CHAIN)
        {
            joinOutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(joinInput);
        }
        else if (inputSplit.getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            joinOutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput);
        }
        else
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, "join algorithm '" +
                    inputSplit.getJoinAlgo() + "' is not supported");
        }
        return joinOutputFuture.whenComplete(((joinOutput, err) ->
        {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(Storage.Scheme.minio, inputSplit.getIncludeCols(), (JoinOutput) joinOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }

    private CompletableFuture<?> getLambdaScanOutput(PixelsSplit inputSplit)
    {
        ScanInput scanInput = new ScanInput();
        scanInput.setQueryId(inputSplit.getQueryId());
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName(inputSplit.getTableName());
        tableInfo.setColumnsToRead(inputSplit.getIncludeCols());
        Pair<Integer, InputSplit> inputSplitPair = getInputSplit(inputSplit);
        tableInfo.setInputSplits(Arrays.asList(inputSplitPair.getRight()));
        TableScanFilter filter = createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), inputSplit.getIncludeCols(), inputSplit.getConstraint());
        tableInfo.setFilter(JSON.toJSONString(filter));
        scanInput.setTableInfo(tableInfo);
        // logger.info("table scan filter: " + tableInfo.getFilter());
        String folder = config.getMinioOutputFolderForQuery(inputSplit.getQueryId());
        String endpoint = config.getMinioEndpoint();
        String accessKey = config.getMinioAccessKey();
        String secretKey = config.getMinioSecretKey();
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, endpoint, accessKey, secretKey);
        OutputInfo outputInfo = new OutputInfo(folder, true, storageInfo, true);
        scanInput.setOutput(outputInfo);

        return InvokerFactory.Instance().getInvoker(WorkerType.SCAN)
                .invoke(scanInput).whenComplete(((scanOutput, err) -> {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(Storage.Scheme.minio, inputSplit.getIncludeCols(), (ScanOutput) scanOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }
}
