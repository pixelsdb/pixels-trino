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
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.physical.Storage;
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
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
    private final String connectorId;
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsTrinoConfig config;

    @Inject
    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy, PixelsTrinoConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.config = requireNonNull(config, "config is null");
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
        logger.info("getTableHandle is called on table " + tableName.getTableName());
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
                        PixelsTableHandle.TableType.BASE, null);
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
        logger.info("getColumnHandles is called on table " + pixelsTableHandle.getTableName());
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
            throw new TableNotFoundException(pixelsTableHandle.toSchemaTableName());
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
        logger.info("listTableColumns is called on table " + prefix.getTable().get());
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
        String storageScheme = ((String) Optional.ofNullable(tableMetadata.getProperties().get("storage"))
                .orElse("hdfs")).toLowerCase(); // use HDFS by default.
        if (!Storage.Scheme.isValid(storageScheme))
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                    "Unsupported storage scheme '" + storageScheme + "'.");
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
            boolean res = this.metadataProxy.createTable(schemaName, tableName, storageScheme, columns);
            if (res == false && ignoreExisting == false)
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
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal _owner)
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
        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;
        logger.info("applyFilter is called on table " + tableHandle.getTableName());
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
            // returns empty means reject filter pushdown.
            return Optional.empty();
        }

        tableHandle = new PixelsTableHandle(tableHandle.getConnectorId(),
                tableHandle.getSchemaName(), tableHandle.getTableName(),
                tableHandle.getTableAlias(), tableHandle.getColumns(), newDomain,
                tableHandle.getTableType(), tableHandle.getJoinHandle());

        // pushing down without statistics pre-calculation.
        return Optional.of(new ConstraintApplicationResult<>(tableHandle, remainingFilter, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;
        StringBuilder assignmentsBuilder = new StringBuilder();
        for (String column : assignments.keySet())
        {
            assignmentsBuilder.append(column).append("::")
                    .append(((PixelsColumnHandle)assignments.get(column)).getColumnName()).append(",");
        }
        StringBuilder projectionsBuilder = new StringBuilder();
        for (ConnectorExpression project : projections)
        {
            projectionsBuilder.append(project.toString()).append(",");
        }
        logger.info("applyProjection is called on table " + tableHandle.getTableName() +
                " with assignments " + assignmentsBuilder + " and projects " + projectionsBuilder);
        List<PixelsColumnHandle> newColumns = assignments.values().stream()
                .map(PixelsColumnHandle.class::cast).collect(toImmutableList());

        Set<PixelsColumnHandle> newColumnSet = ImmutableSet.copyOf(newColumns);
        Set<PixelsColumnHandle> tableColumnSet = ImmutableSet.copyOf(tableHandle.getColumns());
        if (newColumnSet.equals(tableColumnSet))
        {
            return Optional.empty();
        }

        verify(tableColumnSet.containsAll(newColumnSet),
                "applyProjection called with columns %s and some are not available in existing query: %s",
                newColumnSet, tableColumnSet);

        return Optional.of(new ProjectionApplicationResult<>(
                new PixelsTableHandle(connectorId, tableHandle.getSchemaName(),
                        tableHandle.getTableName(),tableHandle.getTableAlias(),
                        newColumns, tableHandle.getConstraint(), tableHandle.getTableType(),
                        tableHandle.getJoinHandle()),
                projections,
                assignments.entrySet().stream().map(assignment -> new Assignment(
                        assignment.getKey(), assignment.getValue(),
                        ((PixelsColumnHandle) assignment.getValue()).getColumnType()
                )).collect(toImmutableList()),
                false)); // pushing down without statistics pre-calculation.
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        if (!config.isLambdaEnabled())
        {
            //return TableStatistics.empty();
        }
        TableStatistics.Builder builder = TableStatistics.builder();
        PixelsTableHandle table = (PixelsTableHandle) tableHandle;
        switch (table.getTableName())
        {
            case "orders":
                builder.setRowCount(Estimate.of(150000000.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(11.0/9*1024*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "customer":
                builder.setRowCount(Estimate.of(15000000.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(2.1/8*1024*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "lineitem":
                builder.setRowCount(Estimate.of(600037902.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(46.5/16*1024*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "supplier":
                builder.setRowCount(Estimate.of(1000000.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(135.0/7*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "part":
                builder.setRowCount(Estimate.of(20000000.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(1.2/9*1024*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "partsupp":
                builder.setRowCount(Estimate.of(80000000.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(10.4/5*1024*1024*1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "nation":
                builder.setRowCount(Estimate.of(25.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(1024));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
            case "region":
                builder.setRowCount(Estimate.of(5.0));
                for (PixelsColumnHandle column : table.getColumns())
                {
                    ColumnStatistics.Builder columnBuilder = ColumnStatistics.builder();
                    //columnBuilder.setDataSize(Estimate.of(256));
                    columnBuilder.setNullsFraction(Estimate.zero());
                    builder.setColumnStatistics(column, columnBuilder.build());
                }
                break;
        }
        return builder.build();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) handle;
        logger.info("aggregation push down on: " + tableHandle.getSchemaName() + "." + tableHandle.getTableName());
        for (ColumnHandle columnHandle : assignments.values())
        {
            logger.info("aggregation assignment: " + columnHandle.toString());
        }
        return ConnectorMetadata.super.applyAggregation(session, handle, aggregates, assignments, groupingSets);
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
        if (!config.isLambdaEnabled())
        {
            return Optional.empty();
        }

        PixelsTableHandle leftTable = (PixelsTableHandle) left;
        PixelsTableHandle rightTable = (PixelsTableHandle) right;
        if (joinConditions.size() > 1 ||
                joinConditions.get(0).getOperator() != JoinCondition.Operator.EQUAL)
        {
            // We only support single equal-join.
            logger.info("[join push down is rejected for multi-join-condition].");
            return Optional.empty();
        }
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
                logger.info("[join push down is rejected for unsupported join type (" + joinType + ")]");
                return Optional.empty();
        }
        // create the join handle.
        JoinCondition joinCondition = joinConditions.get(0);
        Optional<PixelsColumnHandle> leftKeyColumn = getVariableColumnHandle(
                leftAssignments, joinCondition.getLeftExpression());
        Optional<PixelsColumnHandle> rightKeyColumn = getVariableColumnHandle(
                rightAssignments, joinCondition.getRightExpression());
        if (leftKeyColumn.isEmpty() || rightKeyColumn.isEmpty())
        {
            logger.info("[join push down is rejected for missing join keys].");
            return Optional.empty();
        }
        PixelsJoinHandle joinHandle = new PixelsJoinHandle(
                leftTable, leftKeyColumn.get(), rightTable, rightKeyColumn.get(), pixelsJoinType);

        // generate the joinedColumns and new left/right column handles.
        int logicalOrdinal = 0;
        ImmutableMap.Builder<ColumnHandle, ColumnHandle> newLeftColumnsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<PixelsColumnHandle> joinedColumns = ImmutableList.builder();
        StringBuilder builder = new StringBuilder("[join push down] - ");
        builder.append("left table: ").append(leftTable.getTableName()).append(", key column: ")
                .append(leftKeyColumn.get().getColumnName()).append("(")
                .append(leftKeyColumn.get().getLogicalOrdinal()).append(")").append(", columns: ");
        for (PixelsColumnHandle column : leftTable.getColumns())
        {
            builder.append(column.getColumnName()).append("(").append(column.getLogicalOrdinal()).append(")").append(":");
            PixelsColumnHandle newColumn = PixelsColumnHandle.builderFrom(column)
                    .setLogicalOrdinal(logicalOrdinal++).build();
            newLeftColumnsBuilder.put(column, newColumn);
            joinedColumns.add(newColumn);
        }
        builder.append(", assignments: ");
        for (ColumnHandle column : leftAssignments.values())
        {
            builder.append(((PixelsColumnHandle)column).getColumnName()).append("(")
                    .append(((PixelsColumnHandle)column).getLogicalOrdinal()).append(")").append(":");
        }
        ImmutableMap.Builder<ColumnHandle, ColumnHandle> newRightColumnsBuilder = ImmutableMap.builder();
        builder.append(", right table: ").append(rightTable.getTableName()).append(", key column: ")
                .append(rightKeyColumn.get().getColumnName()).append("(")
                .append(rightKeyColumn.get().getLogicalOrdinal()).append(")").append(", columns: ");
        for (PixelsColumnHandle column : rightTable.getColumns())
        {
            builder.append(column.getColumnName()).append("(").append(column.getLogicalOrdinal()).append(")").append(":");
            PixelsColumnHandle newColumn = PixelsColumnHandle.builderFrom(column)
                    .setLogicalOrdinal(logicalOrdinal++).build();
            newRightColumnsBuilder.put(column, newColumn);
            joinedColumns.add(newColumn);
        }
        builder.append(", assignments: ");
        for (ColumnHandle column : rightAssignments.values())
        {
            builder.append(((PixelsColumnHandle)column).getColumnName()).append("(")
                    .append(((PixelsColumnHandle)column).getLogicalOrdinal()).append(")").append(":");
        }
        // setup schema and table names.
        String schemaName = "join_" + UUID.randomUUID().toString().replace("-", "");
        String tableName = leftTable.getTableName() + "_join_" + rightTable.getTableName();
        PixelsTableHandle joinedTableHandle = new PixelsTableHandle(
                connectorId, schemaName, tableName, tableName, joinedColumns.build(), TupleDomain.all(),
                PixelsTableHandle.TableType.JOINED, joinHandle);
        builder.append(", joined schema: ").append(schemaName).append(", joined table: ").append(tableName)
                .append(", join type: ").append(pixelsJoinType);
        logger.info(builder.toString());

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
        // TODO: API change in Trino 315 https://trino.io/docs/current/release/release-315.html:
        //  Allow connectors to provide view definitions. ConnectorViewDefinition now contains the real view definition
        //  rather than an opaque blob. Connectors that support view storage can use the JSON representation of that
        //  class as a stable storage format. The JSON representation is the same as the previous opaque blob, thus
        //  all existing view definitions will continue to work. (https://github.com/trinodb/trino/pull/976)
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating views");
//        try
//        {
//            boolean res = this.pixelsMetadataProxy.createView(viewName.getSchemaName(), viewName.getTableName(), viewData);
//            if (res == false)
//            {
//                throw  new TrinoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
//                        "Failed to create view '" + viewName + "'.");
//            }
//        } catch (MetadataException e)
//        {
//            throw new TrinoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
//        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try
        {
            boolean res = this.metadataProxy.dropView(viewName.getSchemaName(), viewName.getTableName());
            if (res == false)
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
}
