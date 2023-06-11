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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.View;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.stats.RangeStats;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author tao
 * @author hank
 * @author tianxiao
 **/
public class PixelsMetadata implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(PixelsMetadata.class);
    private static final JsonCodec<ConnectorViewDefinition> ViewCodec =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    private final String connectorId;
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsTrinoConfig config;
    private final PixelsTransactionHandle transHandle;

    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy,
                          PixelsTrinoConfig config, PixelsTransactionHandle transHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.config = requireNonNull(config, "config is null");
        this.transHandle = requireNonNull(transHandle, "transHandle is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try
        {
            return this.metadataProxy.existSchema(schemaName);
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNamesInternal();
    }

    private List<String> listSchemaNamesInternal()
    {
        List<String> schemaNameList = null;
        try
        {
            schemaNameList = metadataProxy.getSchemaNames();
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return schemaNameList;
    }

    @Override
    public PixelsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try
        {
            if (this.metadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
            {
                List<PixelsColumnHandle> columns;
                try
                {
                    // initially, get all the columns from the table.
                    columns = metadataProxy.getTableColumn(
                            connectorId, tableName.getSchemaName(), tableName.getTableName());
                } catch (MetadataException e)
                {
                    throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
                }
                PixelsTableHandle tableHandle = new PixelsTableHandle(
                        connectorId, tableName.getSchemaName(), tableName.getTableName(),
                        tableName.getTableName() + "_" + UUID.randomUUID()
                                .toString().replace("-", ""),
                        columns, TupleDomain.all(), // match all tuples at the beginning.
                        Table.TableType.BASE, null, null);
                return tableHandle;
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        checkArgument(tableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");
        if (tableHandle.getColumns() != null)
        {
            List<PixelsColumnHandle> columnHandleList = tableHandle.getColumns();
            List<ColumnMetadata> columns = columnHandleList.stream().map(PixelsColumnHandle::getColumnMetadata)
                    .collect(toImmutableList());
            return new ConnectorTableMetadata(
                    new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()), columns);
        }
        return getTableMetadataInternal(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    private ConnectorTableMetadata getTableMetadataInternal(String schemaName, String tableName)
    {
        List<PixelsColumnHandle> columnHandleList;
        try
        {
            columnHandleList = metadataProxy.getTableColumn(connectorId, schemaName, tableName);
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        List<ColumnMetadata> columns = columnHandleList.stream().map(PixelsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = metadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * PIXELS-179:
                 * Only try to get table names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (metadataProxy.existSchema(schema))
                {
                    for (String table : metadataProxy.getTableNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        checkArgument(pixelsTableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");

        List<PixelsColumnHandle> columnHandleList;
        if (((PixelsTableHandle) tableHandle).getColumns() != null)
        {
            columnHandleList = ((PixelsTableHandle) tableHandle).getColumns();
        }
        else
        {
            try
            {
                columnHandleList = metadataProxy.getTableColumn(
                        connectorId, pixelsTableHandle.getSchemaName(), pixelsTableHandle.getTableName());
            } catch (MetadataException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
            }
        }
        if (columnHandleList == null)
        {
            throw new TableNotFoundException(pixelsTableHandle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (PixelsColumnHandle column : columnHandleList)
        {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        if (prefix.getSchema().isPresent())
        {
            SchemaTableName tableName = new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get());
            try
            {
                /**
                 * PIXELS-183:
                 * Return an empty result if the table does not exist.
                 * This is possible when reading the content of information_schema tables.
                 */
                if (metadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
                {
                    ConnectorTableMetadata tableMetadata = getTableMetadataInternal(
                            tableName.getSchemaName(), tableName.getTableName());
                    // table can disappear during listing operation
                    if (tableMetadata != null)
                    {
                        columns.put(tableName, tableMetadata.getColumns());
                    }
                }
            } catch (MetadataException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        return ((PixelsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        String storage = (String) tableMetadata.getProperties().get("storage");
        String orderedPaths = (String) tableMetadata.getProperties().get("ordered_paths");
        String compactPaths = (String) tableMetadata.getProperties().get("compact_paths");
        if (storage == null)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Table must be created with the property 'storage'.");
        }
        if (orderedPaths == null)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Table must be created with the property 'ordered_paths'.");
        }
        if (compactPaths == null)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Table must be created with the property 'compact_paths'.");
        }
        if (!Storage.Scheme.isValid(storage))
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Unsupported storage scheme '" + storage + "'.");
        }
        Storage.Scheme storageScheme = Storage.Scheme.from(storage);
        String[] orderedPathUris = orderedPaths.split(";");
        for (int i = 0; i < orderedPathUris.length; ++i)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(orderedPathUris[i]);
            if (scheme == null)
            {
                orderedPathUris[i] = storageScheme + "://" + orderedPathUris[i];
            }
            if (scheme != storageScheme)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                        "The storage schemes in 'ordered_paths' are inconsistent with 'storage'.");
            }
        }
        String[] compactPathUris = compactPaths.split(";");
        for (int i = 0; i < compactPathUris.length; ++i)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(compactPathUris[i]);
            if (scheme == null)
            {
                compactPathUris[i] = storageScheme + "://" + compactPathUris[i];
            }
            if (scheme != storageScheme)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                        "The storage schemes in 'compact_paths' are inconsistent with 'storage'.");
            }
        }
        List<Column> columns = new ArrayList<>();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns())
        {
            Column column = new Column();
            column.setName(columnMetadata.getName());
            // columnMetadata.getType().getDisplayName(); is the same as
            // columnMetadata.getType().getTypeSignature().toString();
            column.setType(columnMetadata.getType().getDisplayName());
            // column size is set to 0 when the table is just created.
            column.setSize(0);
            columns.add(column);
        }
        try
        {
            boolean res = this.metadataProxy.createTable(schemaName, tableName, storageScheme,
                    Arrays.asList(orderedPathUris), Arrays.asList(compactPathUris), columns);
            if (!res && !ignoreExisting)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table '" + schemaTableName + "' might already exist, failed to create it.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        String schemaName = pixelsTableHandle.getSchemaName();
        String tableName = pixelsTableHandle.getTableName();

        try
        {
            boolean res = this.metadataProxy.dropTable(schemaName, tableName);
            if (res == false)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table " + schemaName + "." + tableName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName,
                             Map<String, Object> properties, TrinoPrincipal _owner)
    {
        try
        {
            boolean res = this.metadataProxy.createSchema(schemaName);
            if (res == false)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " already exists.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try
        {
            boolean res = this.metadataProxy.dropSchema(schemaName);
            if (res == false)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        return ConnectorMetadata.super.applyLimit(session, handle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        if (transHandle.getExecutorType() != ExecutorType.CF)
        {
            // Issue #60: Trino's filters are currently more efficient, so pushdown should only be used for Lambda.
            return Optional.empty();
        }

        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;

        TupleDomain<PixelsColumnHandle> oldDomain = tableHandle.getConstraint();
        TupleDomain<PixelsColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary()
            .transformKeys(PixelsColumnHandle.class::cast));
        TupleDomain<ColumnHandle> remainingFilter;

        if (newDomain.isNone())
        {
            // all is the default constraint.
            remainingFilter = TupleDomain.all();
        }
        else
        {
            remainingFilter = TupleDomain.withColumnDomains(new HashMap<>());
        }

        if (oldDomain.equals(newDomain))
        {
            // returns empty means reject filter push down.
            logger.debug("[filter push down is rejected on " + newDomain.toString(session) + "]");
            return Optional.empty();
        }

        tableHandle = new PixelsTableHandle(tableHandle.getConnectorId(),
                tableHandle.getSchemaName(), tableHandle.getTableName(),
                tableHandle.getTableAlias(), tableHandle.getColumns(), newDomain,
                tableHandle.getTableType(), tableHandle.getJoinHandle(),
                tableHandle.getAggrHandle());

        // pushing down without statistics pre-calculation.
        logger.debug("filter push down on " + newDomain.toString(session));
        return Optional.of(new ConstraintApplicationResult<>(tableHandle, remainingFilter, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;

        List<PixelsColumnHandle> newColumns = assignments.values().stream()
                .map(PixelsColumnHandle.class::cast).collect(toImmutableList());
        List<PixelsColumnHandle> tableColumns = tableHandle.getColumns();

        Set<PixelsColumnHandle> newColumnSet = ImmutableSet.copyOf(newColumns);
        Set<PixelsColumnHandle> tableColumnSet = ImmutableSet.copyOf(tableColumns);
        if (newColumnSet.equals(tableColumnSet))
        {
            logger.debug("[projection push down is rejected]");
            return Optional.empty();
        }

        verify(tableColumnSet.containsAll(newColumnSet),
                "applyProjection called with columns %s and some are not available in existing query: %s",
                newColumnSet, tableColumnSet);

        logger.debug("projection push down on table " + tableHandle.getTableName());
        return Optional.of(new ProjectionApplicationResult<>(
                new PixelsTableHandle(connectorId, tableHandle.getSchemaName(),
                        tableHandle.getTableName(),tableHandle.getTableAlias(),
                        newColumns, tableHandle.getConstraint(), tableHandle.getTableType(),
                        tableHandle.getJoinHandle(), tableHandle.getAggrHandle()),
                projections,
                assignments.entrySet().stream().map(assignment -> new Assignment(
                        assignment.getKey(), assignment.getValue(),
                        ((PixelsColumnHandle) assignment.getValue()).getColumnType()
                )).collect(toImmutableList()),
                false)); // pushing down without statistics pre-calculation.
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        TableStatistics.Builder tableStatBuilder = TableStatistics.builder();
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        try
        {
            // PIXELS-423: Trino may try to get statistics on synthetic tables, for example on tpch-q7.
            if (!metadataProxy.existTable(tableHandle.getSchemaName(), tableHandle.getTableName()))
            {
                return TableStatistics.empty();
            }
        } catch (MetadataException e)
        {
            logger.error(e, "failed to check existence of '" +
                    tableHandle.getSchemaTableName() + "' through metadata service");
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        List<Column> columns = metadataProxy.getColumnStatistics(tableHandle.getSchemaName(), tableHandle.getTableName());
        requireNonNull(columns, "columns is null");
        Map<String, Column> columnMap = new HashMap<>(columns.size());
        for (Column column : columns)
        {
            columnMap.put(column.getName(), column);
        }

        try
        {
            long rowCount = metadataProxy.getTable(tableHandle.getSchemaName(), tableHandle.getTableName()).getRowCount();
            tableStatBuilder.setRowCount(Estimate.of(rowCount));
            logger.debug("table '" + tableHandle.getTableName() + "' row count: " + rowCount);
        } catch (MetadataException e)
        {
            logger.error(e, "failed to get table from metadata service");
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }

        for (PixelsColumnHandle columnHandle : tableHandle.getColumns())
        {
            ColumnStatistics.Builder columnStatsBuilder = ColumnStatistics.builder();
            Column column = columnMap.get(columnHandle.getColumnName());
            columnStatsBuilder.setDataSize(Estimate.of(column.getSize()));
            columnStatsBuilder.setNullsFraction(Estimate.of(column.getNullFraction()));
            columnStatsBuilder.setDistinctValuesCount(Estimate.of(column.getCardinality()));
            try
            {
                TypeDescription pixelsType = metadataProxy.parsePixelsType(columnHandle.getColumnType());
                ByteBuffer statsBytes = column.getRecordStats().slice();
                if (statsBytes != null && statsBytes.remaining() > 0)
                {
                    PixelsProto.ColumnStatistic columnStatsPb = PixelsProto.ColumnStatistic.parseFrom(statsBytes);
                    StatsRecorder statsRecorder = StatsRecorder.create(pixelsType, columnStatsPb);
                    if (statsRecorder instanceof RangeStats<?> rangeStats)
                    {
                        /**
                         * When needed, we can also get a general range stats like this:
                         * GeneralRangeStats rangeStats = StatsRecorder.toGeneral(pixelsType, (RangeStats<?>) statsRecorder);
                         * The double min/max in general range stats is the readable representation of the min/max value.
                         */
                        logger.debug(column.getName() + " column range: {min:" + rangeStats.getMinimum() + ", max:" +
                                rangeStats.getMaximum() + ", hasMin:" + rangeStats.hasMinimum() +
                                ", hasMax:" + rangeStats.hasMaximum() + "}");
                        columnStatsBuilder.setRange(DoubleRange.from(columnHandle.getColumnType(),
                                rangeStats.getMinimum(), rangeStats.getMaximum()));
                    }
                }
            } catch (InvalidProtocolBufferException e)
            {
                logger.error(e, "failed to parse record statistics from protobuf object");
            }
            tableStatBuilder.setColumnStatistics(columnHandle, columnStatsBuilder.build());
        }

        return tableStatBuilder.build();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        if (transHandle.getExecutorType() != ExecutorType.CF)
        {
            return Optional.empty();
        }

        checkArgument(!groupingSets.isEmpty(), "group sets is empty");

        if (groupingSets.size() > 1)
        {
            logger.debug("[aggregation push down is rejected: not support multi-grouping-sets]");
            return Optional.empty();
        }

        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;

        if (tableHandle.getTableType() != Table.TableType.BASE)
        {
            // TODO: support aggregation on joined table.
            logger.debug("[aggregation push down is rejected: not support aggregation on joined table]");
            return Optional.empty();
        }

        logger.debug("aggregation push down on: " + tableHandle.getSchemaName() + "." + tableHandle.getTableName());

        ImmutableList.Builder<PixelsColumnHandle> newColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableList.Builder<PixelsColumnHandle> aggrColumns = ImmutableList.builder();
        ImmutableList.Builder<PixelsColumnHandle> aggrResultColumns = ImmutableList.builder();
        ImmutableList.Builder<FunctionType> aggrFunctionTypes = ImmutableList.builder();

        List<PixelsColumnHandle> groupKeyColumns = groupingSets.get(0).stream()
                .map(PixelsColumnHandle.class::cast).collect(toImmutableList());

        List<PixelsColumnHandle> tableColumns = tableHandle.getColumns();

        Set<PixelsColumnHandle> columnSet = ImmutableSet.copyOf(tableColumns);

        int logicalOrdinal = 0;

        for (PixelsColumnHandle groupKey : groupKeyColumns)
        {
            checkArgument(columnSet.contains(groupKey),
                    "grouping key %s is not included in the table columns: %s",
                    groupKey, tableColumns);
            newColumnsBuilder.add(groupKey);
        }

        for (AggregateFunction aggregate : aggregates)
        {
            if (aggregate.getFilter().isPresent())
            {
                logger.debug("[aggregation push down is rejected: filter on aggregate is not supported]");
                return Optional.empty();
            }
            if (aggregate.isDistinct())
            {
                logger.debug("[aggregation push down is rejected: distinct on aggregate is not supported]");
                return Optional.empty();
            }
            if (!aggregate.getSortItems().isEmpty())
            {
                logger.debug("[aggregation push down is rejected: sort on aggregate is not supported]");
                return Optional.empty();
            }
            if (aggregate.getArguments().size() != 1)
            {
                logger.debug("[aggregation push down is rejected: multi-column aggregation with '" +
                        aggregate.getArguments().size() + "' arguments is not supported]");
                return Optional.empty();
            }
            if (!FunctionType.isSupported(aggregate.getFunctionName()))
            {
                logger.debug("[aggregation push down is rejected: function type '" +
                        aggregate.getFunctionName() + ". is not supported]");
                return Optional.empty();
            }
            ConnectorExpression aggrArgument = aggregate.getArguments().get(0);
            Optional<PixelsColumnHandle> aggrColumn = getVariableColumnHandle(assignments, aggrArgument);
            if (aggrColumn.isEmpty())
            {
                logger.debug("[aggregation push down is rejected: aggregate argument is not a single column]");
                return Optional.empty();
            }
            aggrColumns.add(aggrColumn.get());
            String newColumnName = aggregate.getFunctionName() + "_" + aggrColumn.get().getColumnName() + "_"
                    + UUID.randomUUID().toString().replace("-", "");
            TypeDescription pixelsType = metadataProxy.parsePixelsType(aggregate.getOutputType());
            PixelsColumnHandle newColumn = aggrColumn.get().toBuilder()
                    .setColumnName(newColumnName).setColumnAlias(newColumnName)
                    .setColumnType(aggregate.getOutputType()).setTypeCategory(pixelsType.getCategory())
                    .setColumnComment("synthetic").setLogicalOrdinal(logicalOrdinal++).build();
            newColumnsBuilder.add(newColumn);
            aggrResultColumns.add(newColumn);
            aggrFunctionTypes.add(FunctionType.fromName(aggregate.getFunctionName()));
            projections.add(new Variable(newColumn.getColumnName(), newColumn.getColumnType()));
            resultAssignments.add(new Assignment(newColumn.getColumnName(), newColumn, newColumn.getColumnType()));
        }

        List<PixelsColumnHandle> newColumns = newColumnsBuilder.build();

        PixelsAggrHandle aggrHandle = new PixelsAggrHandle(aggrColumns.build(), aggrResultColumns.build(),
                groupKeyColumns, newColumns, aggrFunctionTypes.build(), tableHandle);

        String newSchemaName = "aggregate_" + UUID.randomUUID().toString().replace("-", "");
        String newTableName = "aggregate_" + tableHandle.getTableName();
        PixelsTableHandle newHandle = new PixelsTableHandle(
                connectorId, newSchemaName, newTableName, newTableName, newColumns,
                TupleDomain.all(), Table.TableType.AGGREGATED, null, aggrHandle);

        return Optional.of(new AggregationApplicationResult<>(newHandle, projections.build(),
                resultAssignments.build(), ImmutableMap.of(), false));
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
            ConnectorSession session, JoinType joinType,
            ConnectorTableHandle left, ConnectorTableHandle right,
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        if (transHandle.getExecutorType() != ExecutorType.CF)
        {
            return Optional.empty();
        }

        PixelsTableHandle leftTable = (PixelsTableHandle) left;
        PixelsTableHandle rightTable = (PixelsTableHandle) right;
        logger.debug("join push down: left=" + leftTable.getTableName() +
                ", right=" + rightTable.getTableName());

        if ((leftTable.getTableType() != Table.TableType.BASE && leftTable.getTableType() != Table.TableType.JOINED) ||
                (rightTable.getTableType() != Table.TableType.BASE && rightTable.getTableType() != Table.TableType.JOINED))
        {
            logger.debug("[join push down is rejected: only base or joined tables are currently supported in join].");
            return Optional.empty();
        }

        // get the join keys.
        ImmutableList.Builder<PixelsColumnHandle> leftKeyColumns =
                ImmutableList.builderWithExpectedSize(joinConditions.size());
        ImmutableList.Builder<PixelsColumnHandle> rightKeyColumns =
                ImmutableList.builderWithExpectedSize(joinConditions.size());
        for (JoinCondition joinCondition : joinConditions)
        {
            if (joinCondition.getOperator() != JoinCondition.Operator.EQUAL)
            {
                logger.debug("[join push down is rejected for not supporting non-equal join].");
                return Optional.empty();
            }
            Optional<PixelsColumnHandle> leftKeyColumn = getVariableColumnHandle(
                    leftAssignments, joinCondition.getLeftExpression());
            Optional<PixelsColumnHandle> rightKeyColumn = getVariableColumnHandle(
                    rightAssignments, joinCondition.getRightExpression());
            if (leftKeyColumn.isEmpty() || rightKeyColumn.isEmpty())
            {
                logger.debug("[join push down is rejected for failed to parse join conditions].");
                return Optional.empty();
            }
            leftKeyColumns.add(leftKeyColumn.get());
            rightKeyColumns.add(rightKeyColumn.get());
        }

        // get the join type.
        io.pixelsdb.pixels.executor.join.JoinType pixelsJoinType;
        switch (joinType)
        {
            case INNER:
                pixelsJoinType = io.pixelsdb.pixels.executor.join.JoinType.EQUI_INNER;
                break;
            case LEFT_OUTER:
                pixelsJoinType = io.pixelsdb.pixels.executor.join.JoinType.EQUI_LEFT;
                break;
            case RIGHT_OUTER:
                pixelsJoinType = io.pixelsdb.pixels.executor.join.JoinType.EQUI_RIGHT;
                break;
            case FULL_OUTER:
                pixelsJoinType = io.pixelsdb.pixels.executor.join.JoinType.EQUI_FULL;
                break;
            default:
                // We only support the above types of joins.
                logger.debug("[join push down is rejected for unsupported join type (" + joinType + ")]");
                return Optional.empty();
        }

        // generate the joinedColumns and new left/right column handles.
        int logicalOrdinal = 0;
        ImmutableMap.Builder<ColumnHandle, ColumnHandle> newLeftColumnsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<PixelsColumnHandle> joinedColumns = ImmutableList.builder();
        for (PixelsColumnHandle column : leftTable.getColumns())
        {
            PixelsColumnHandle newColumn = column.toBuilder().setLogicalOrdinal(logicalOrdinal++).build();
            newLeftColumnsBuilder.put(column, newColumn);
            joinedColumns.add(newColumn);
        }
        ImmutableMap.Builder<ColumnHandle, ColumnHandle> newRightColumnsBuilder = ImmutableMap.builder();
        for (PixelsColumnHandle column : rightTable.getColumns())
        {
            PixelsColumnHandle newColumn = column.toBuilder().setLogicalOrdinal(logicalOrdinal++).build();
            newRightColumnsBuilder.put(column, newColumn);
            joinedColumns.add(newColumn);
        }

        // setup schema and table names.
        String schemaName = "join_" + UUID.randomUUID().toString().replace("-", "");
        String tableName = leftTable.getTableName() + "_join_" + rightTable.getTableName();

        // create the join handle.
        PixelsJoinHandle joinHandle = new PixelsJoinHandle(
                leftTable, leftKeyColumns.build(), rightTable, rightKeyColumns.build(), pixelsJoinType);

        PixelsTableHandle joinedTableHandle = new PixelsTableHandle(
                connectorId, schemaName, tableName, tableName, joinedColumns.build(), TupleDomain.all(),
                Table.TableType.JOINED, joinHandle, null);

        return Optional.of(new JoinApplicationResult<>(
                joinedTableHandle,
                newLeftColumnsBuilder.build(),
                newRightColumnsBuilder.build(),
                false));
    }

    private static Optional<PixelsColumnHandle> getVariableColumnHandle(
            Map<String, ColumnHandle> assignments, ConnectorExpression expression)
    {
        requireNonNull(assignments, "assignments is null");
        requireNonNull(expression, "expression is null");
        if (!(expression instanceof Variable))
        {
            return Optional.empty();
        }

        String name = ((Variable) expression).getName();
        ColumnHandle columnHandle = assignments.get(name);
        verifyNotNull(columnHandle, "No assignment for %s", name);
        return Optional.of(((PixelsColumnHandle) columnHandle));
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        ConnectorMetadata.super.validateScan(session, handle);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName,
                           ConnectorViewDefinition definition, boolean replace)
    {
        /**
         * Issue #6:
         * API change in Trino 315 https://trino.io/docs/current/release/release-315.html:
         * Allow connectors to provide view definitions. ConnectorViewDefinition now contains the real view definition
         * rather than an opaque blob. Connectors that support view storage can use the JSON representation of that
         * class as a stable storage format. The JSON representation is the same as the previous opaque blob, thus
         * all existing view definitions will continue to work. (https://github.com/trinodb/trino/pull/976)
         */
        String viewData = ViewCodec.toJson(definition);
        try
        {
            boolean res = this.metadataProxy.createView(
                    viewName.getSchemaName(), viewName.getTableName(), viewData, replace);
            if (!res)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Failed to create view '" + viewName + "'.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try
        {
            boolean res = this.metadataProxy.dropView(viewName.getSchemaName(), viewName.getTableName());
            if (!res)
            {
                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "View '" + viewName.getSchemaName() + "." + viewName.getTableName() + "' does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = metadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * PIXELS-194:
                 * Only try to get view names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (metadataProxy.existSchema(schema))
                {
                    for (String table : metadataProxy.getViewNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        try
        {
            /**
             * Issue #6:
             * getView is called to check view existence when creating the view, therefore, it should
             * not throw exception if the view does not exist.
             */
            View view = metadataProxy.getView(viewName.getSchemaName(), viewName.getTableName(), true);
            if (view != null)
            {
                return Optional.of(ViewCodec.fromJson(view.getData()));
            }
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return Optional.empty();
    }

    /**
     * Gets the definitions of views, possibly filtered by schema.
     * This optional method may be implemented by connectors that can support fetching
     * view data in bulk. It is used to implement {@code information_schema.views}.
     *
     * @param session the connector session.
     * @param filterSchema get all the views in the connector if not present.
     */
    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> builder = ImmutableMap.builder();
        try
        {
            if (filterSchema.isPresent())
            {
                String schemaName = filterSchema.get();
                if (this.metadataProxy.existSchema(schemaName))
                {
                    /**
                     * PIXELS-194:
                     * Only try to get views if the schema exists.
                     * Otherwise, return an empty set, which is required by Presto
                     * when reading the content of information_schema tables.
                     */
                    List<View> views = this.metadataProxy.getViews(schemaName);
                    for (View view : views)
                    {
                        SchemaTableName stName = new SchemaTableName(schemaName, view.getName());
                        builder.put(stName, ViewCodec.fromJson(view.getData()));
                    }
                }
            }
            else
            {
                /**
                 * Issue #6:
                 * Get all the views in Pixels if filterSchema is not present.
                 */
                for (String schemaName : metadataProxy.getSchemaNames())
                {
                    for (View view : metadataProxy.getViews(schemaName))
                    {
                        SchemaTableName stName = new SchemaTableName(schemaName, view.getName());
                        builder.put(stName, ViewCodec.fromJson(view.getData()));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }
}
