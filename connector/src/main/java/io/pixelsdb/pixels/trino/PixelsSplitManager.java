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
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.PixelsExecutor;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.join.JoinAdvisor;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.lambda.AggregationOperator;
import io.pixelsdb.pixels.executor.lambda.JoinOperator;
import io.pixelsdb.pixels.executor.lambda.Operator;
import io.pixelsdb.pixels.executor.lambda.domain.InputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.InputSplit;
import io.pixelsdb.pixels.executor.lambda.input.AggregationInput;
import io.pixelsdb.pixels.executor.lambda.input.JoinInput;
import io.pixelsdb.pixels.executor.plan.*;
import io.pixelsdb.pixels.executor.plan.Table.TableType;
import io.pixelsdb.pixels.executor.predicate.Bound;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.trino.exception.CacheException;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.properties.PixelsSessionProperties;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @author guodong
 * @author tao
 */
public class PixelsSplitManager implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(PixelsSplitManager.class);
    private final String connectorId;
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsTrinoConfig config;
    private final boolean cacheEnabled;
    private final boolean multiSplitForOrdered;
    private final boolean projectionReadEnabled;
    private final String cacheSchema;
    private final String cacheTable;
    private final int fixedSplitSize;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy,
                              PixelsTrinoConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.config = requireNonNull(config, "config is null");
        String cacheEnabled = config.getConfigFactory().getProperty("cache.enabled");
        String projectionReadEnabled = config.getConfigFactory().getProperty("projection.read.enabled");
        String multiSplit = config.getConfigFactory().getProperty("multi.split.for.ordered");
        this.fixedSplitSize = Integer.parseInt(config.getConfigFactory().getProperty("fixed.split.size"));
        this.cacheEnabled = Boolean.parseBoolean(cacheEnabled);
        this.projectionReadEnabled = Boolean.parseBoolean(projectionReadEnabled);
        this.multiSplitForOrdered = Boolean.parseBoolean(multiSplit);
        this.cacheSchema = config.getConfigFactory().getProperty("cache.schema");
        this.cacheTable = config.getConfigFactory().getProperty("cache.table");
    }

    public static List<PixelsColumnHandle> getIncludeColumns(PixelsTableHandle tableHandle)
    {
        return getIncludeColumns(tableHandle.getColumns(), tableHandle);
    }

    public static List<PixelsColumnHandle> getIncludeColumns(List<PixelsColumnHandle> columns, PixelsTableHandle tableHandle)
    {
        ImmutableList.Builder<PixelsColumnHandle> builder = ImmutableList.builder();
        builder.addAll(columns);
        if (tableHandle.getConstraint().getDomains().isPresent())
        {
            Set<PixelsColumnHandle> columnSet = new HashSet<>(columns);
            for (PixelsColumnHandle column : tableHandle.getConstraint().getDomains().get().keySet())
            {
                if (!columnSet.contains(column))
                {
                    builder.add(column);
                }
            }
        }
        return builder.build();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle trans,
                                          ConnectorSession session,
                                          ConnectorTableHandle handle,
                                          SplitSchedulingStrategy splitSchedulingStrategy,
                                          DynamicFilter dynamicFilter,
                                          Constraint extra)
    {
        PixelsTransactionHandle transHandle = (PixelsTransactionHandle) trans;
        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;
        if (tableHandle.getTableType() == TableType.BASE)
        {
            List<PixelsSplit> pixelsSplits = getScanSplits(transHandle, session, tableHandle);
            Collections.shuffle(pixelsSplits);
            return new PixelsSplitSource(pixelsSplits);
        }

        /*
         * Start building Join or Aggregation splits.
         *
         * Do not directly use the column names from the root joined table like this:
         * String[] includeCols = root.getColumnNames();
         * Because Trino assumes the join result remains the same column order as
         * tableHandle.getColumns(), but root.getColumnNames() might not follow this order.
         */
        String[] includeCols = new String[tableHandle.getColumns().size()];
        for (int i = 0; i < tableHandle.getColumns().size(); ++i)
        {
            // Use the synthetic column name to access the join result.
            includeCols[i] = tableHandle.getColumns().get(i).getSynthColumnName();
        }
        List<String> columnOrder = ImmutableList.of();
        List<String> cacheOrder = ImmutableList.of();
        // The address is not used to dispatch Pixels splits, so we use set it the localhost.
        HostAddress address = HostAddress.fromString("localhost:8080");
        TupleDomain<PixelsColumnHandle> emptyConstraint = Constraint.alwaysTrue().getSummary().transformKeys(
                columnHandle -> (PixelsColumnHandle) columnHandle);
        if (tableHandle.getTableType() == TableType.JOINED)
        {
            // The table type is joined, means lambda has been enabled.
            JoinedTable root = parseJoinPlan(tableHandle);
            logger.info("join plan: " + JSON.toJSONString(root));

            try
            {
                boolean orderedPathEnabled = PixelsSessionProperties.getOrderedPathEnabled(session);
                boolean compactPathEnabled = PixelsSessionProperties.getCompactPathEnabled(session);
                // Call executor to execute this join plan.
                PixelsExecutor executor = new PixelsExecutor(
                        transHandle.getTransId(), root, orderedPathEnabled, compactPathEnabled);
                // Ensure multi-pipeline join is supported.
                JoinOperator joinOperator = (JoinOperator) executor.getRootOperator();
                logger.info("join operator: " + JSON.toJSONString(joinOperator));
                joinOperator.executePrev();

                Thread outputCollector = new Thread(() -> {
                    try
                    {
                        JoinOperator.JoinOutputCollection outputCollection = joinOperator.collectOutputs();
                        SerializerFeature[] features = new SerializerFeature[]{SerializerFeature.WriteClassName};
                        String json = JSON.toJSONString(outputCollection, features);
                        logger.info("join outputs: " + json);
                    } catch (Exception e)
                    {
                        logger.error(e, "failed to execute the join plan using pixels-lambda");
                        throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                                "failed to execute the join plan using pixels-lambda");
                    }
                });
                outputCollector.start();

                // Build the splits of the join result.
                ImmutableList.Builder<PixelsSplit> splitsBuilder = ImmutableList.builder();
                for (JoinInput joinInput : joinOperator.getJoinInputs())
                {
                    PixelsSplit split = new PixelsSplit(
                            transHandle.getTransId(), connectorId, root.getSchemaName(), root.getTableName(),
                            Storage.Scheme.minio.name(), joinInput.getOutput().getFileNames(),
                            Collections.nCopies(joinInput.getOutput().getFileNames().size(), 0),
                            Collections.nCopies(joinInput.getOutput().getFileNames().size(), -1),
                            false, false, Arrays.asList(address), columnOrder,
                            cacheOrder, emptyConstraint, TableType.JOINED, joinOperator.getJoinAlgo(),
                            JSON.toJSONString(joinInput), null);
                    splitsBuilder.add(split);
                }
                return new PixelsSplitSource(splitsBuilder.build());
            } catch (IOException | MetadataException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, e);
            }
        }
        else if (tableHandle.getTableType() == TableType.AGGREGATED)
        {
            AggregatedTable root = parseAggregatePlan(tableHandle, transHandle);
            logger.info("aggregation plan: " + JSON.toJSONString(root));
            try
            {
                boolean orderedPathEnabled = PixelsSessionProperties.getOrderedPathEnabled(session);
                boolean compactPathEnabled = PixelsSessionProperties.getCompactPathEnabled(session);
                // Call executor to execute this aggregation plan.
                PixelsExecutor executor = new PixelsExecutor(
                        transHandle.getTransId(), root, orderedPathEnabled, compactPathEnabled);
                AggregationOperator aggrOperator = (AggregationOperator) executor.getRootOperator();
                logger.info("aggregation operator: " + JSON.toJSONString(aggrOperator));
                aggrOperator.executePrev();

                Thread outputCollector = new Thread(() -> {
                    try
                    {
                        Operator.OutputCollection outputCollection = aggrOperator.collectOutputs();
                        SerializerFeature[] features = new SerializerFeature[]{SerializerFeature.WriteClassName};
                        String json = JSON.toJSONString(outputCollection, features);
                        logger.info("aggregation outputs: " + json);
                    } catch (Exception e)
                    {
                        logger.error(e, "failed to execute the aggregation plan using pixels-lambda");
                        throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                                "failed to execute the aggregation plan using pixels-lambda");
                    }
                });
                outputCollector.start();

                // Build the split of the aggregation result.
                AggregationInput aggrInput = aggrOperator.getFinalAggrInput();
                PixelsSplit split = new PixelsSplit(
                        transHandle.getTransId(), connectorId, root.getSchemaName(), root.getTableName(),
                        Storage.Scheme.minio.name(), ImmutableList.of(aggrInput.getOutput().getPath()),
                        ImmutableList.of(0), ImmutableList.of(-1), false, false,
                        Arrays.asList(address), columnOrder, cacheOrder, emptyConstraint, TableType.AGGREGATED,
                        null, null, JSON.toJSONString(aggrInput));
                return new PixelsSplitSource(ImmutableList.of(split));
            } catch (IOException | MetadataException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR, e);
            }
        }
        else
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_CONNECTOR_ERROR, "table type is not supported");
        }
    }

    /**
     * Parse the join plan from the joined table handle. In the parsed join plan, we create the
     * synthetic column names for the result of each join. These synthetic column names can be used
     * to read the join result, and are generated from the column name plus the logical ordinary id
     * of the columns from the left and right tables.
     *
     * @param tableHandle the joined table handle
     * @return the parsed join plan
     */
    private JoinedTable parseJoinPlan(PixelsTableHandle tableHandle)
    {
        checkArgument(tableHandle.getTableType() == TableType.JOINED,
                "tableHandle is not a joined table");
        PixelsJoinHandle joinHandle = tableHandle.getJoinHandle();

        PixelsTableHandle leftHandle = joinHandle.getLeftTable();
        PixelsTableHandle rightHandle = joinHandle.getRightTable();
        List<PixelsColumnHandle> leftColumnHandles;
        List<PixelsColumnHandle> rightColumnHandles;
        String[] leftColumns, rightColumns;
        io.pixelsdb.pixels.executor.plan.Table leftTable;
        io.pixelsdb.pixels.executor.plan.Table rightTable;

        if (leftHandle.getTableType() == TableType.BASE)
        {
            leftColumnHandles = getIncludeColumns(leftHandle);
            leftColumns = new String[leftColumnHandles.size()];
            for (int i = 0; i < leftColumns.length; ++i)
            {
                // Do not use synthetic column name for base table.
                leftColumns[i] = leftColumnHandles.get(i).getColumnName();
            }
            leftTable = new BaseTable(leftHandle.getSchemaName(), leftHandle.getTableName(),
                    leftHandle.getTableAlias(), leftColumns, createTableScanFilter(leftHandle.getSchemaName(),
                    leftHandle.getTableName(), leftColumns, leftHandle.getConstraint()));
        }
        else
        {
            checkArgument(joinHandle.getLeftTable().getTableType() == TableType.JOINED,
                    "left table is not a base or joined table, can not parse");
            // recursive deep left.
            leftTable = parseJoinPlan(joinHandle.getLeftTable());
            leftColumns = leftTable.getColumnNames();
            List<PixelsColumnHandle> columnHandles = getIncludeColumns(leftHandle);
            ImmutableList.Builder<PixelsColumnHandle> leftColumnHandlesBuilder = ImmutableList.builder();
            for (int i = 0; i < leftColumns.length; ++i)
            {
                for (int j = 0; j < leftColumns.length; ++j)
                {
                    // The left table is a joined table, the leftColumns are the synthetic column names.
                    if (leftColumns[i].equals(columnHandles.get(j).getSynthColumnName()))
                    {
                        leftColumnHandlesBuilder.add(columnHandles.get(j));
                    }
                }
            }
            leftColumnHandles = leftColumnHandlesBuilder.build();
        }

        if (rightHandle.getTableType() == TableType.BASE)
        {
            rightColumnHandles = getIncludeColumns(rightHandle);
            rightColumns = new String[rightColumnHandles.size()];
            for (int i = 0; i < rightColumns.length; ++i)
            {
                // Do not use synthetic column name for base table.
                rightColumns[i] = rightColumnHandles.get(i).getColumnName();
            }
            rightTable = new BaseTable(rightHandle.getSchemaName(), rightHandle.getTableName(),
                    rightHandle.getTableAlias(), rightColumns, createTableScanFilter(rightHandle.getSchemaName(),
                    rightHandle.getTableName(), rightColumns, rightHandle.getConstraint()));
        }
        else
        {
            checkArgument(joinHandle.getRightTable().getTableType() == TableType.JOINED,
                    "right table is not a base or joined table, can not parse");
            // recursive deep right.
            rightTable = parseJoinPlan(joinHandle.getRightTable());
            rightColumns = rightTable.getColumnNames();
            List<PixelsColumnHandle> columnHandles = getIncludeColumns(rightHandle);
            ImmutableList.Builder<PixelsColumnHandle> rightColumnHandlesBuilder = ImmutableList.builder();
            for (int i = 0; i < rightColumns.length; ++i)
            {
                for (int j = 0; j < rightColumns.length; ++j)
                {
                    // The right table is a joined table, the rightColumns are the synthetic column names.
                    if (rightColumns[i].equals(columnHandles.get(j).getSynthColumnName()))
                    {
                        rightColumnHandlesBuilder.add(columnHandles.get(j));
                    }
                }
            }
            rightColumnHandles = rightColumnHandlesBuilder.build();
        }

        // Parse the left and right key columns, projections, and joined columns.
        List<PixelsColumnHandle> leftJoinedColumnHandles = new LinkedList<>();
        List<PixelsColumnHandle> rightJoinedColumnHandles = new ArrayList<>();
        Map<PixelsColumnHandle, PixelsColumnHandle> leftKeyColumnMap =
                new HashMap<>(joinHandle.getLeftKeyColumns().size());
        Map<PixelsColumnHandle, PixelsColumnHandle> rightKeyColumnMap =
                new HashMap<>(joinHandle.getRightKeyColumns().size());
        for (PixelsColumnHandle leftKeyColumn : joinHandle.getLeftKeyColumns())
        {
            leftKeyColumnMap.put(leftKeyColumn, leftKeyColumn);
        }
        for (PixelsColumnHandle rightKeyColumn : joinHandle.getRightKeyColumns())
        {
            rightKeyColumnMap.put(rightKeyColumn, rightKeyColumn);
        }
        int[] leftKeyColumnIds = new int[joinHandle.getLeftKeyColumns().size()];
        int[] rightKeyColumnIds = new int[joinHandle.getRightKeyColumns().size()];
        int leftKeysIndex = 0, rightKeysIndex = 0;

        Map<PixelsColumnHandle, PixelsColumnHandle> joinedColumnHandleMap =
                new HashMap<>(tableHandle.getColumns().size());
        for (PixelsColumnHandle joinedColumn : tableHandle.getColumns())
        {
            joinedColumnHandleMap.put(joinedColumn, joinedColumn);
        }
        /*
         * We must ensure that left/right joined column handles remain the same
         * column order of left/right column handles.
         */
        boolean[] leftProjection = new boolean[leftColumnHandles.size()];
        for (int i = 0; i < leftProjection.length; ++i)
        {
            PixelsColumnHandle leftColumnHandle = leftColumnHandles.get(i);
            PixelsColumnHandle leftKeyColumn = leftKeyColumnMap.get(leftColumnHandle);
            if (leftKeyColumn != null && leftColumnHandle.getLogicalOrdinal() == leftKeyColumn.getLogicalOrdinal())
            {
                leftKeyColumnIds[leftKeysIndex++] = i;
            }
            leftProjection[i] = joinedColumnHandleMap.containsKey(leftColumnHandle);
            if (leftProjection[i])
            {
                leftJoinedColumnHandles.add(joinedColumnHandleMap.get(leftColumnHandle));
            }
        }
        boolean[] rightProjection = new boolean[rightColumnHandles.size()];
        for (int i = 0; i < rightProjection.length; ++i)
        {
            PixelsColumnHandle rightColumnHandle = rightColumnHandles.get(i);
            PixelsColumnHandle rightKeyColumn = rightKeyColumnMap.get(rightColumnHandle);
            if (rightKeyColumn != null && rightColumnHandle.getLogicalOrdinal() == rightKeyColumn.getLogicalOrdinal())
            {
                rightKeyColumnIds[rightKeysIndex++] = i;
            }
            rightProjection[i] = joinedColumnHandleMap.containsKey(rightColumnHandle);
            if (rightProjection[i])
            {
                rightJoinedColumnHandles.add(joinedColumnHandleMap.get(rightColumnHandle));
            }
        }

        /*
         * leftJoinedColumns and rightJoinedColumns are used to build the column names in the
         * join result, so we should use the synthetic column names here.
         */
        String[] leftJoinedColumns = leftJoinedColumnHandles.stream()
                .map(PixelsColumnHandle::getSynthColumnName)
                .collect(Collectors.toUnmodifiableList()).toArray(new String[0]);
        String[] rightJoinedColumns = rightJoinedColumnHandles.stream()
                .map(PixelsColumnHandle::getSynthColumnName)
                .collect(Collectors.toUnmodifiableList()).toArray(new String[0]);

        Join join;
        JoinEndian joinEndian;
        JoinAlgorithm joinAlgo;
        try
        {
            joinEndian = JoinAdvisor.Instance().getJoinEndian(leftTable, rightTable);
            joinAlgo = JoinAdvisor.Instance().getJoinAlgorithm(leftTable, rightTable, joinEndian);
        } catch (MetadataException e)
        {
            logger.error("failed to get join algorithm", e);
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }

        boolean rotateLeftRight = false;
        if (leftHandle.getTableType() == TableType.BASE &&
                rightHandle.getTableType() == TableType.JOINED)
        {
            /*
             * If the right table is a joined table and the left table is a base table, we have to rotate
             * the two tables to ensure the generated join plan is deep-left. This is for the convenience
             * of join execution.
             */
            rotateLeftRight = true;
        }
        else if (leftHandle.getTableType() == TableType.JOINED &&
                rightHandle.getTableType() == TableType.JOINED &&
                joinEndian == JoinEndian.LARGE_LEFT)
        {
            /*
             * If the both left and right tables are joined tables, and the left table is larger than the
             * right table, we have to rotate the two tables to ensure the small table is on the left.
             * This is for the convenience of join execution.
             */
            rotateLeftRight = true;
        }

        if (rotateLeftRight)
        {
            join = new Join(rightTable, leftTable, rightJoinedColumns, leftJoinedColumns,
                    rightKeyColumnIds, leftKeyColumnIds, rightProjection, leftProjection,
                    joinEndian.flip(), joinHandle.getJoinType().flip(), joinAlgo);
        }
        else
        {
            join = new Join(leftTable, rightTable, leftJoinedColumns, rightJoinedColumns,
                    leftKeyColumnIds, rightKeyColumnIds, leftProjection, rightProjection,
                    joinEndian, joinHandle.getJoinType(), joinAlgo);
        }

        return new JoinedTable(tableHandle.getSchemaName(), tableHandle.getTableName(),
                tableHandle.getTableAlias(), join);
    }

    private AggregatedTable parseAggregatePlan(PixelsTableHandle tableHandle, PixelsTransactionHandle transHandle)
    {
        PixelsAggrHandle aggrHandle = tableHandle.getAggrHandle();
        PixelsTableHandle originTableHandle = aggrHandle.getOriginTable();
        List<PixelsColumnHandle> originColumns = originTableHandle.getColumns();

        // groupKeyColumns and aggrColumns are from the origin table.
        int numGroupKeyColumns = aggrHandle.getGroupKeyColumns().size();
        int numAggrColumns = aggrHandle.getAggrColumns().size();
        int numOutputColumns = aggrHandle.getOutputColumns().size();
        Map<PixelsColumnHandle, PixelsColumnHandle> groupKeyColumnMap = new HashMap<>(numGroupKeyColumns);
        Map<PixelsColumnHandle, PixelsColumnHandle> aggrColumnMap = new HashMap<>(numAggrColumns);
        Map<PixelsColumnHandle, PixelsColumnHandle> outputColumnMap = new HashMap<>(numOutputColumns);
        for (PixelsColumnHandle groupKeyColumn : aggrHandle.getGroupKeyColumns())
        {
            groupKeyColumnMap.put(groupKeyColumn, groupKeyColumn);
        }
        for (PixelsColumnHandle aggrColumn : aggrHandle.getAggrColumns())
        {
            aggrColumnMap.put(aggrColumn, aggrColumn);
        }

        // Build the group-key column projection.
        List<PixelsColumnHandle> outputColumns = aggrHandle.getOutputColumns();
        Set<PixelsColumnHandle> newColumns = ImmutableSet.copyOf(tableHandle.getColumns());
        boolean[] groupKeyColumnProj = new boolean[numGroupKeyColumns];
        for (int i = 0, j = 0; i < outputColumns.size(); ++i)
        {
            PixelsColumnHandle outputColumn = outputColumns.get(i);
            outputColumnMap.put(outputColumn, outputColumn);
            if (groupKeyColumnMap.containsKey(outputColumn))
            {
                if (newColumns.contains(outputColumn))
                {
                    groupKeyColumnProj[j] = true;
                }
                else
                {
                    groupKeyColumnProj[j] = false;
                }
                ++j;
            }
        }

        // Build the result column alias and types.
        List<PixelsColumnHandle> resultColumns = aggrHandle.getAggrResultColumns();
        String[] resultColumnAlias = new String[resultColumns.size()];
        String[] resultColumnTypes = new String[resultColumns.size()];
        for (int i = 0; i < resultColumns.size(); ++i)
        {
            PixelsColumnHandle resultColumn = resultColumns.get(i);
            /*
             * The column name of result column is already synthetic. But the includeCols
             * used in the PixelsSplit uses synthetic column name, thus we must also use
             * synthetic column name here to match the includeCols.
             */
            resultColumnAlias[i] = resultColumn.getSynthColumnName();
            // Display name can be parsed into Pixels types.
            resultColumnTypes[i] = resultColumn.getColumnType().getDisplayName();
        }

        // Build the column ids and the group-key column alias.
        int[] groupKeyColumnIds = new int[numGroupKeyColumns];
        String[] groupKeyColumnAlias = new String[numGroupKeyColumns];
        int[] aggregateColumnIds = new int[numAggrColumns];
        for (int i = 0, j = 0, k = 0; i < originColumns.size(); ++i)
        {
            PixelsColumnHandle origin = originColumns.get(i);
            PixelsColumnHandle groupKey = groupKeyColumnMap.get(origin);
            if (groupKey != null && groupKey.getLogicalOrdinal() == origin.getLogicalOrdinal())
            {
                // Use synthetic column name in the aggregation result.
                PixelsColumnHandle outputColumn = requireNonNull(outputColumnMap.get(groupKey),
                        "group-key column is not found in the output columns");
                groupKeyColumnAlias[j] = outputColumn.getSynthColumnName();
                groupKeyColumnIds[j++] = i;
            }
            PixelsColumnHandle aggrCol = aggrColumnMap.get(origin);
            if (aggrCol != null && aggrCol.getLogicalOrdinal() == origin.getLogicalOrdinal())
            {
                aggregateColumnIds[k++] = i;
            }
        }

        // Build the function types.
        List<FunctionType> functionTypeList = aggrHandle.getFunctionTypes();
        FunctionType[] functionTypes = new FunctionType[functionTypeList.size()];
        for (int i = 0; i < functionTypeList.size(); ++i)
        {
            functionTypes[i] = functionTypeList.get(i);
        }

        // Build the output endpoint.
        OutputEndPoint outputEndPoint = new OutputEndPoint(Storage.Scheme.minio,
                config.getMinioOutputFolderForQuery(transHandle.getTransId(),
                        tableHandle.getSchemaName() + "_" + tableHandle.getTableName()),
                config.getMinioAccessKey(), config.getMinioSecretKey(), config.getMinioEndpoint());

        // Build the origin table.
        io.pixelsdb.pixels.executor.plan.Table originTable;
        if (originTableHandle.getTableType() == TableType.JOINED)
        {
            originTable = parseJoinPlan(originTableHandle);
        }
        else
        {
            originTable = parseBaseTable(originTableHandle);
        }

        Aggregation aggregation = new Aggregation(
                groupKeyColumnAlias, resultColumnAlias, resultColumnTypes, groupKeyColumnProj,
                groupKeyColumnIds, aggregateColumnIds, functionTypes, outputEndPoint, originTable);

        return new AggregatedTable(tableHandle.getSchemaName(), tableHandle.getTableName(),
                tableHandle.getTableAlias(), aggregation);
    }

    private BaseTable parseBaseTable(PixelsTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "baseTableHandle is null");
        checkArgument(tableHandle.getTableType() == TableType.BASE,
                "baseTableHandle is not a handle of base table");
        List<PixelsColumnHandle> columnHandles = getIncludeColumns(tableHandle);
        String[] columns = new String[columnHandles.size()];
        for (int i = 0; i < columns.length; ++i)
        {
            // Do not use synthetic column name for base table.
            columns[i] = columnHandles.get(i).getColumnName();
        }
        return new BaseTable(tableHandle.getSchemaName(), tableHandle.getTableName(),
                tableHandle.getTableAlias(), columns, createTableScanFilter(tableHandle.getSchemaName(),
                tableHandle.getTableName(), columns, tableHandle.getConstraint()));
    }

    public static Pair<Integer, InputSplit> getInputSplit(PixelsSplit split)
    {
        ArrayList<InputInfo> inputInfos = new ArrayList<>();
        int splitSize = 0;
        List<String> paths = split.getPaths();
        List<Integer> rgStarts = split.getRgStarts();
        List<Integer> rgLengths = split.getRgLengths();

        for (int i = 0; i < paths.size(); ++i)
        {
            inputInfos.add(new InputInfo(paths.get(i), rgStarts.get(i), rgLengths.get(i)));
            splitSize += rgLengths.get(i);
        }
        return new Pair<>(splitSize, new InputSplit(inputInfos));
    }

    private List<PixelsSplit> getScanSplits(PixelsTransactionHandle transHandle, ConnectorSession session,
                                            PixelsTableHandle tableHandle)
    {
        // Do not use constraint_ in the parameters, it is always TupleDomain.all().
        TupleDomain<PixelsColumnHandle> constraint = tableHandle.getConstraint();
        // logger.info("constraint from table handle: " + constraint.toString(session));
        List<PixelsColumnHandle> desiredColumns = getIncludeColumns(tableHandle);

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        Table table;
        Storage storage;
        List<Layout> layouts;
        try
        {
            table = metadataProxy.getTable(schemaName, tableName);
            storage = StorageFactory.Instance().getStorage(table.getStorageScheme());
            layouts = metadataProxy.getDataLayouts(schemaName, tableName);
        }
        catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        } catch (IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }

        /**
         * PIXELS-78:
         * Only try to use cache for the cached table.
         * By avoiding cache probing for uncached tables, query performance on
         * uncached tables is improved significantly (10%-20%).
         * this.cacheSchema and this.cacheTable are not null if this.cacheEnabled == true.
         */
        boolean usingCache = false;
        if (this.cacheEnabled)
        {
            if (schemaName.equalsIgnoreCase(this.cacheSchema) &&
                    tableName.equalsIgnoreCase(this.cacheTable))
            {
                usingCache = true;
            }
        }

        /**
         * PIXELS-169:
         * We use session properties to configure if the ordered and compact layout path are enabled.
         */
        boolean orderedPathEnabled = PixelsSessionProperties.getOrderedPathEnabled(session);
        boolean compactPathEnabled = PixelsSessionProperties.getCompactPathEnabled(session);

        List<PixelsSplit> pixelsSplits = new ArrayList<>();
        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            IndexName indexName = new IndexName(schemaName, tableName);
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            ColumnSet columnSet = new ColumnSet();
            for (PixelsColumnHandle column : desiredColumns)
            {
                columnSet.addColumn(column.getColumnName());
            }

            // get split size
            int splitSize;
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            if (this.fixedSplitSize > 0)
            {
                splitSize = this.fixedSplitSize;
            }
            else
            {
                // log.info("columns to be accessed: " + columnSet.toString());
                SplitsIndex splitsIndex = IndexFactory.Instance().getSplitsIndex(indexName);
                if (splitsIndex == null)
                {
                    logger.debug("splits index not exist in factory, building index...");
                    splitsIndex = buildSplitsIndex(order, splits, indexName);
                }
                else
                {
                    int indexVersion = splitsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("splits index version is not up-to-date, updating index...");
                        splitsIndex = buildSplitsIndex(order, splits, indexName);
                    }
                }
                SplitPattern bestSplitPattern = splitsIndex.search(columnSet);
                // log.info("bestPattern: " + bestPattern.toString());
                splitSize = bestSplitPattern.getSplitSize();
            }
            logger.debug("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInBlock();

            // get compact path
            String compactPath;
            if (projectionReadEnabled)
            {
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(indexName);
                Projections projections = JSON.parseObject(layout.getProjections(), Projections.class);
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(order, projections, indexName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(order, projections, indexName);
                    }
                }
                ProjectionPattern projectionPattern = projectionsIndex.search(columnSet);
                if (projectionPattern != null)
                {
                    logger.debug("suitable projection pattern is found, path='" + projectionPattern.getPath() + '\'');
                    compactPath = projectionPattern.getPath();
                }
                else
                {
                    compactPath = layout.getCompactPath();
                }
            }
            else
            {
                compactPath = layout.getCompactPath();
            }
            logger.debug("using compact path: " + compactPath);

            if(usingCache)
            {
                Compact compact = layout.getCompactObject();
                int cacheBorder = compact.getCacheBorder();
                List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
                String cacheVersion;
                EtcdUtil etcdUtil = EtcdUtil.Instance();
                KeyValue keyValue = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if(keyValue != null)
                {
                    // 1. get version
                    cacheVersion = keyValue.getValue().toString(StandardCharsets.UTF_8);
                    logger.debug("cache version: " + cacheVersion);
                    // 2. get the cached files of each node
                    List<KeyValue> nodeFiles = etcdUtil.getKeyValuesByPrefix(
                            Constants.CACHE_LOCATION_LITERAL + cacheVersion);
                    if(nodeFiles.size() > 0)
                    {
                        Map<String, String> fileToNodeMap = new HashMap<>();
                        for (KeyValue kv : nodeFiles)
                        {
                            String node = kv.getKey().toString(StandardCharsets.UTF_8).split("_")[2];
                            String[] files = kv.getValue().toString(StandardCharsets.UTF_8).split(";");
                            for(String file : files)
                            {
                                fileToNodeMap.put(file, node);
                                // log.info("cache location: {file='" + file + "', node='" + node + "'");
                            }
                        }
                        try
                        {
                            // 3. add splits in orderedPath
                            if (orderedPathEnabled)
                            {
                                List<String> orderedPaths = storage.listPaths(layout.getOrderPath());

                                int numPath = orderedPaths.size();
                                for (int i = 0; i < numPath; )
                                {
                                    int firstPath = i; // the path of the first ordered file in the split.
                                    List<String> paths = new ArrayList<>(this.multiSplitForOrdered ? splitSize : 1);
                                    if (this.multiSplitForOrdered)
                                    {
                                        for (int j = 0; j < splitSize && i < numPath; ++j, ++i)
                                        {
                                            paths.add(orderedPaths.get(i));
                                        }
                                    } else
                                    {
                                        paths.add(orderedPaths.get(i++));
                                    }

                                    // We do not cache files in the ordered path, thus get locations from the storage.
                                    List<HostAddress> orderedAddresses = toHostAddresses(
                                            storage.getLocations(orderedPaths.get(firstPath)));

                                    PixelsSplit pixelsSplit = new PixelsSplit(
                                            transHandle.getTransId(), connectorId,
                                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                                            table.getStorageScheme(), paths,
                                            Collections.nCopies(paths.size(), 0),
                                            Collections.nCopies(paths.size(), 1),
                                            false, storage.hasLocality(), orderedAddresses,
                                            order.getColumnOrder(), new ArrayList<>(0),
                                            constraint, TableType.BASE, null, null, null);
                                    // log.debug("Split in orderPath: " + pixelsSplit.toString());
                                    pixelsSplits.add(pixelsSplit);
                                }
                            }
                            // 4. add splits in compactPath
                            if (compactPathEnabled)
                            {
                                int curFileRGIdx;
                                List<String> compactPaths = storage.listPaths(compactPath);
                                for (String path : compactPaths)
                                {
                                    curFileRGIdx = 0;
                                    while (curFileRGIdx < rowGroupNum)
                                    {
                                        String node = fileToNodeMap.get(path);
                                        List<HostAddress> compactAddresses;
                                        boolean ensureLocality;
                                        if (node == null)
                                        {
                                            // this file is not cached, get the locations from the storage.
                                            compactAddresses = toHostAddresses(storage.getLocations(path));
                                            ensureLocality = storage.hasLocality();
                                        } else
                                        {
                                            // this file is cached.
                                            ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
                                            builder.add(HostAddress.fromString(node));
                                            compactAddresses = builder.build();
                                            ensureLocality = true;
                                        }

                                        PixelsSplit pixelsSplit = new PixelsSplit(
                                                transHandle.getTransId(), connectorId,
                                                tableHandle.getSchemaName(), tableHandle.getTableName(),
                                                table.getStorageScheme(), Arrays.asList(path),
                                                Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                                true, ensureLocality, compactAddresses, order.getColumnOrder(),
                                                cacheColumnletOrders, constraint, TableType.BASE,
                                                null, null, null);
                                        pixelsSplits.add(pixelsSplit);
                                        // log.debug("Split in compactPath" + pixelsSplit.toString());
                                        curFileRGIdx += splitSize;
                                    }
                                }
                            }
                        }
                        catch (IOException e)
                        {
                            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
                        }
                    }
                    else
                    {
                        logger.error("Get caching files error when version is " + cacheVersion);
                        throw new TrinoException(PixelsErrorCode.PIXELS_CACHE_NODE_FILE_ERROR, new CacheException());
                    }
                }
                else
                {
                    throw new TrinoException(PixelsErrorCode.PIXELS_CACHE_VERSION_ERROR, new CacheException());
                }
            }
            else
            {
                logger.debug("cache is disabled or no cache available on this table");
                try
                {
                    // 1. add splits in orderedPath
                    if (orderedPathEnabled)
                    {
                        List<String> orderedPaths = storage.listPaths(layout.getOrderPath());

                        int numPath = orderedPaths.size();
                        for (int i = 0; i < numPath; )
                        {
                            int firstPath = i;
                            List<String> paths = new ArrayList<>(this.multiSplitForOrdered ? splitSize : 1);
                            if (this.multiSplitForOrdered)
                            {
                                for (int j = 0; j < splitSize && i < numPath; ++j, ++i)
                                {
                                    paths.add(orderedPaths.get(i));
                                }
                            } else
                            {
                                paths.add(orderedPaths.get(i++));
                            }

                            List<HostAddress> orderedAddresses = toHostAddresses(
                                    storage.getLocations(orderedPaths.get(firstPath)));

                            PixelsSplit pixelsSplit = new PixelsSplit(
                                    transHandle.getTransId(), connectorId,
                                    tableHandle.getSchemaName(), tableHandle.getTableName(),
                                    table.getStorageScheme(), paths,
                                    Collections.nCopies(paths.size(), 0),
                                    Collections.nCopies(paths.size(), 1),
                                    false, storage.hasLocality(), orderedAddresses,
                                    order.getColumnOrder(), new ArrayList<>(0),
                                    constraint, TableType.BASE, null, null, null);
                            // logger.debug("Split in orderPath: " + pixelsSplit.toString());
                            pixelsSplits.add(pixelsSplit);
                        }
                    }
                    // 2. add splits in compactPath
                    if (compactPathEnabled)
                    {
                        List<String> compactPaths = storage.listPaths(compactPath);

                        int curFileRGIdx;
                        for (String path : compactPaths)
                        {
                            curFileRGIdx = 0;
                            while (curFileRGIdx < rowGroupNum)
                            {
                                List<HostAddress> compactAddresses = toHostAddresses(storage.getLocations(path));

                                PixelsSplit pixelsSplit = new PixelsSplit(
                                        transHandle.getTransId(), connectorId,
                                        tableHandle.getSchemaName(), tableHandle.getTableName(),
                                        table.getStorageScheme(), Arrays.asList(path),
                                        Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                        false, storage.hasLocality(), compactAddresses,
                                        order.getColumnOrder(), new ArrayList<>(0),
                                        constraint, TableType.BASE, null, null, null);
                                pixelsSplits.add(pixelsSplit);
                                curFileRGIdx += splitSize;
                            }
                        }
                    }
                }
                catch (IOException e)
                {
                    throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
                }
            }
        }

        return pixelsSplits;
    }

    public static TableScanFilter createTableScanFilter(
            String schemaName, String tableName,
            String[] includeCols, TupleDomain<PixelsColumnHandle> constraint)
    {
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        TableScanFilter tableScanFilter = new TableScanFilter(schemaName, tableName, columnFilters);
        Map<String, Integer> colToCid = new HashMap<>(includeCols.length);
        for (int i = 0; i < includeCols.length; ++i)
        {
            colToCid.put(includeCols[i], i);
        }
        if (constraint.getDomains().isPresent())
        {
            Map<PixelsColumnHandle, Domain> domains = constraint.getDomains().get();
            for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet())
            {
                ColumnFilter<?> columnFilter = createColumnFilter(entry.getKey(), entry.getValue());
                if (colToCid.containsKey(entry.getKey().getColumnName()))
                {
                    columnFilters.put(colToCid.get(entry.getKey().getColumnName()), columnFilter);
                }
                else
                {
                    throw new TrinoException(PixelsErrorCode.PIXELS_CONNECTOR_ERROR,
                            "column '" + entry.getKey().getColumnName() + "' does not exist in the includeCols");
                }
            }
        }

        return tableScanFilter;
    }

    private static  <T extends Comparable<T>> ColumnFilter<T> createColumnFilter(
            PixelsColumnHandle columnHandle, Domain domain)
    {
        Type prestoType = domain.getType();
        String columnName = columnHandle.getColumnName();
        TypeDescription.Category columnType = columnHandle.getTypeCategory();
        Class<?> filterJavaType = columnType.getInternalJavaType() == byte[].class ?
                String.class : columnType.getInternalJavaType();
        boolean isAll = domain.isAll();
        boolean isNone = domain.isNone();
        boolean allowNull = domain.isNullAllowed();
        boolean onlyNull = domain.isOnlyNull();

        Filter<T> filter = domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    Filter<T> res = new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull);
                    if (ranges.getRangeCount() > 0)
                    {
                        ranges.getOrderedRanges().forEach(range ->
                        {
                            if (range.isSingleValue())
                            {
                                Bound<?> bound = createBound(prestoType, Bound.Type.INCLUDED,
                                        range.getSingleValue());
                                res.addDiscreteValue((Bound<T>) bound);
                            } else
                            {
                                Bound.Type lowerBoundType = range.isLowInclusive() ?
                                        Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                                Bound.Type upperBoundType = range.isHighInclusive() ?
                                        Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                                Object lowerBoundValue = null, upperBoundValue = null;
                                if (range.isLowUnbounded())
                                {
                                    lowerBoundType = Bound.Type.UNBOUNDED;
                                } else
                                {
                                    lowerBoundValue = range.getLowBoundedValue();
                                }
                                if (range.isHighUnbounded())
                                {
                                    upperBoundType = Bound.Type.UNBOUNDED;
                                } else
                                {
                                    upperBoundValue = range.getHighBoundedValue();
                                }
                                Bound<?> lowerBound = createBound(prestoType, lowerBoundType, lowerBoundValue);
                                Bound<?> upperBound = createBound(prestoType, upperBoundType, upperBoundValue);
                                res.addRange((Bound<T>) lowerBound, (Bound<T>) upperBound);
                            }
                        });
                    }
                    return res;
                },
                discreteValues -> {
                    Filter<T> res = new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull);
                    Bound.Type boundType = discreteValues.isInclusive() ?
                            Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                    discreteValues.getValues().forEach(value ->
                    {
                        if (value == null)
                        {
                            throw new TrinoException(PixelsErrorCode.PIXELS_INVALID_METADATA,
                                    "discrete value is null");
                        } else
                        {
                            Bound<?> bound = createBound(prestoType, boundType, value);
                            res.addDiscreteValue((Bound<T>) bound);
                        }
                    });
                    return res;
                },
                allOrNone -> new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull)
        );
        return new ColumnFilter<>(columnName, columnType, filter);
    }

    private static Bound<?> createBound(Type prestoType, Bound.Type boundType, Object value)
    {
        Class<?> javaType = prestoType.getJavaType();
        Bound<?> bound = null;
        if (boundType == Bound.Type.UNBOUNDED)
        {
            bound = new Bound<>(boundType, null);
        }
        else
        {
            requireNonNull(value, "the value of the bound is null");
            if (javaType == long.class)
            {
                switch (prestoType.getTypeSignature().getBase())
                {
                    case StandardTypes.DATE:
                    case StandardTypes.TIME:
                        bound = new Bound<>(boundType, ((Long) value).intValue());
                        break;
                    default:
                        bound = new Bound<>(boundType, (Long) value);
                        break;
                }
            }
            else if (javaType == int.class)
            {
                bound = new Bound<>(boundType, (Integer) value);
            }
            else if (javaType == double.class)
            {
                bound = new Bound<>(boundType, Double.doubleToLongBits((Double) value));
            }
            else if (javaType == boolean.class)
            {
                bound = new Bound<>(boundType, (byte) ((Boolean) value ? 1 : 0));
            }
            else if (javaType == Slice.class)
            {
                bound = new Bound<>(boundType, ((Slice) value).toString(StandardCharsets.UTF_8).trim());
            }
            else
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                        "unsupported data type for filter bound: " + javaType.getName());
            }
        }

        return bound;
    }

    private List<HostAddress> toHostAddresses(List<Location> locations)
    {
        ImmutableList.Builder<HostAddress> addressBuilder = ImmutableList.builder();
        for (Location location : locations)
        {
            for (String host : location.getHosts())
            {
                addressBuilder.add(HostAddress.fromString(host));
            }
        }
        return addressBuilder.build();
    }

    private SplitsIndex buildSplitsIndex(Order order, Splits splits, IndexName indexName) {
        List<String> columnOrder = order.getColumnOrder();
        SplitsIndex index;
        index = new InvertedSplitsIndex(columnOrder, SplitPattern.buildPatterns(columnOrder, splits),
                splits.getNumRowGroupInBlock());
        IndexFactory.Instance().cacheSplitsIndex(indexName, index);
        return index;
    }

    private ProjectionsIndex buildProjectionsIndex(Order order, Projections projections, IndexName indexName) {
        List<String> columnOrder = order.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(indexName, index);
        return index;
    }
}