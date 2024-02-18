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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.vector.lshnns.CachedLSHIndex;
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
import static java.util.stream.Collectors.toList;

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
        List<PixelsColumnHandle> pixelsColumns = columns.stream()
                .map(PixelsColumnHandle.class::cast).collect(toList());

        try
        {
            List<PixelsColumnHandle> columnHandles = tableHandle.getColumns();
            if (columnHandles.size()==1
                    && columnHandles.get(0).getTypeCategory() == TypeDescription.Category.VECTOR) {
                // update the LSH index in every node
                CachedLSHIndex.getInstance().setCurrColumn(columnHandles.get(0));
            }
            if (pixelsSplit.getTableType() == Table.TableType.AGGREGATED)
            {
                // perform aggregation push down.
                Storage storage = StorageFactory.Instance().getStorage(config.getOutputStorageScheme());
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaAggrOutput(pixelsSplit), null);
            }
            if (pixelsSplit.getTableType() == Table.TableType.JOINED)
            {
                // perform join push down.
                Storage storage = StorageFactory.Instance().getStorage(config.getOutputStorageScheme());
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaJoinOutput(pixelsSplit), null);
            }
            if (pixelsSplit.getTableType() == Table.TableType.BASE)
            {
                // perform scan push down.
                List<PixelsColumnHandle> withFilterColumns = getIncludeColumns(pixelsColumns, tableHandle);
                PixelsTransactionHandle transHandle = (PixelsTransactionHandle) transactionHandle;
                if (transHandle.getExecutorType() == ExecutorType.CF &&
                        this.localSplitCounter.get() >= config.getLocalScanConcurrency()
                        /**
                         * Issue #57:
                         * If the number of columns to read is 0, the spits should not be processed by Lambda.
                         * It usually means that the query is like select count(*) from table.
                         * Such queries can be served on the metadata headers that are cached locally, without touching the data.
                         */
                        && !withFilterColumns.isEmpty())
                {
                    String[] columnsToRead = new String[withFilterColumns.size()];
                    boolean[] projection = new boolean[withFilterColumns.size()];
                    int projectionSize = pixelsColumns.size();
                    for (int i = 0; i < columnsToRead.length; ++i)
                    {
                        columnsToRead[i] = withFilterColumns.get(i).getColumnName();
                        projection[i] = i < projectionSize;
                    }
                    Storage storage = StorageFactory.Instance().getStorage(config.getOutputStorageScheme());
                    IntermediateFileCleaner.Instance().registerStorage(storage);
                    return new PixelsPageSource(pixelsSplit, pixelsColumns, storage, cacheFile, indexFile,
                            pixelsFooterCache, getLambdaScanOutput(pixelsSplit, columnsToRead, projection), null);
                } else
                {
                    this.localSplitCounter.incrementAndGet();
                    Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                    return new PixelsPageSource(pixelsSplit, withFilterColumns, storage,
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
        OutputInfo output = aggrInput.getOutput();
        output.setStorageInfo(config.getOutputStorageInfo());
        output.setPath(config.getOutputFolderForQuery(inputSplit.getTransId()) +
                output.getPath().substring(output.getPath().indexOf(inputSplit.getSchemaName())));
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
                inputSplit.permute(config.getOutputStorageScheme(), (AggregationOutput) aggrOutput);
                logger.info("final aggr output: " + JSON.toJSONString(aggrOutput));
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in lambda output read.", e);
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
        output.setStorageInfo(config.getOutputStorageInfo());
        output.setPath(config.getOutputFolderForQuery(inputSplit.getTransId(),
                inputSplit.getSchemaName() + "/" + inputSplit.getTableName()));
        CompletableFuture<Output> joinOutputFuture;
        // logger.debug("join input: " + JSON.toJSONString(joinInput));
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
                inputSplit.permute(config.getOutputStorageScheme(), (JoinOutput) joinOutput);
                logger.info("final join output: " + JSON.toJSONString(joinOutput));
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in lambda output read.", e);
            }
        }));
    }

    private CompletableFuture<?> getLambdaScanOutput(PixelsSplit inputSplit, String[] columnsToRead, boolean[] projection)
    {
        checkArgument(Storage.Scheme.from(inputSplit.getStorageScheme()).equals(config.getInputStorageScheme()), String.format(
                "the storage scheme of table '%s.%s' is not consistent with the input storage scheme for Pixels Turbo",
                inputSplit.getSchemaName(), inputSplit.getTableName()));
        ScanInput scanInput = new ScanInput();
        scanInput.setTransId(inputSplit.getTransId());
        scanInput.setOperatorName("scan_" + inputSplit.getTableName());
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName(inputSplit.getTableName());
        tableInfo.setColumnsToRead(columnsToRead);
        Pair<Integer, InputSplit> inputSplitPair = getInputSplit(inputSplit);
        tableInfo.setInputSplits(Arrays.asList(inputSplitPair.getRight()));
        TableScanFilter filter = createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), columnsToRead, inputSplit.getConstraint());
        tableInfo.setFilter(JSON.toJSONString(filter));
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(config.getInputStorageInfo());
        scanInput.setTableInfo(tableInfo);
        scanInput.setScanProjection(projection);
        String folder = config.getOutputFolderForQuery(inputSplit.getTransId()) + inputSplit.getSplitId() + "/";
        OutputInfo outputInfo = new OutputInfo(folder, config.getOutputStorageInfo(), true);
        scanInput.setOutput(outputInfo);

        return InvokerFactory.Instance().getInvoker(WorkerType.SCAN)
                .invoke(scanInput).whenComplete(((scanOutput, err) -> {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(config.getOutputStorageScheme(), (ScanOutput) scanOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in lambda output read.", e);
            }
        }));
    }
}
