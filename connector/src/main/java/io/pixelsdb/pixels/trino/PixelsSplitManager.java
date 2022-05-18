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
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.TypeDescription;
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
    private final boolean cacheEnabled;
    private final boolean multiSplitForOrdered;
    private final boolean projectionReadEnabled;
    private final String cacheSchema;
    private final String cacheTable;
    private final int fixedSplitSize;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy, PixelsTrinoConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
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
        ImmutableList.Builder<PixelsColumnHandle> builder = ImmutableList.builder();
        builder.addAll(tableHandle.getColumns());
        if (tableHandle.getConstraint().getDomains().isPresent())
        {
            Set<PixelsColumnHandle> oldColumnSet = new HashSet<>(tableHandle.getColumns());
            for (PixelsColumnHandle column : tableHandle.getConstraint().getDomains().get().keySet())
            {
                if (!oldColumnSet.contains(column))
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
        // Do not use constraint_ in the parameters, it is always TupleDomain.all().
        TupleDomain<PixelsColumnHandle> constraint = tableHandle.getConstraint();
        // logger.info("constraint from table handle: " + constraint.toString(session));
        List<PixelsColumnHandle> desiredColumns = getIncludeColumns(tableHandle);

        String[] includeCols = new String[desiredColumns.size()];
        for (int i = 0; i < desiredColumns.size(); ++i)
        {
            includeCols[i] = desiredColumns.get(i).getColumnName();
        }

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
                                for (int i = 0; i < numPath; ++i)
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
                                        paths.add(orderedPaths.get(i));
                                    }

                                    // We do not cache files in the ordered path, thus get locations from the storage.
                                    List<HostAddress> orderedAddresses = toHostAddresses(
                                            storage.getLocations(orderedPaths.get(firstPath)));

                                    PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                                            table.getStorageScheme(), paths, transHandle.getTransId(),
                                            Collections.nCopies(paths.size(), 0),
                                            Collections.nCopies(paths.size(), 1),
                                            false, storage.hasLocality(), orderedAddresses,
                                            order.getColumnOrder(), new ArrayList<>(0),
                                            includeCols, constraint);
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

                                        PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                                tableHandle.getSchemaName(), tableHandle.getTableName(),
                                                table.getStorageScheme(), Arrays.asList(path), transHandle.getTransId(),
                                                Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                                true, ensureLocality, compactAddresses, order.getColumnOrder(),
                                                cacheColumnletOrders, includeCols, constraint);
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
                        for (int i = 0; i < numPath; ++i)
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
                                paths.add(orderedPaths.get(i));
                            }

                            List<HostAddress> orderedAddresses = toHostAddresses(
                                    storage.getLocations(orderedPaths.get(firstPath)));

                            PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                    tableHandle.getSchemaName(), tableHandle.getTableName(),
                                    table.getStorageScheme(), paths, transHandle.getTransId(),
                                    Collections.nCopies(paths.size(), 0),
                                    Collections.nCopies(paths.size(), 1),
                                    false, storage.hasLocality(), orderedAddresses,
                                    order.getColumnOrder(), new ArrayList<>(0),
                                    includeCols, constraint);
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

                                PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                        tableHandle.getSchemaName(), tableHandle.getTableName(),
                                        table.getStorageScheme(), Arrays.asList(path), transHandle.getTransId(),
                                        Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                        false, storage.hasLocality(), compactAddresses,
                                        order.getColumnOrder(), new ArrayList<>(0),
                                        includeCols, constraint);
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

        Collections.shuffle(pixelsSplits);

        return new PixelsSplitSource(pixelsSplits);
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