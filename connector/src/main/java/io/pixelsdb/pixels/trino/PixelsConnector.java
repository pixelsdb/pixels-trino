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

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.QueryTransInfo;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.turbo.planner.autoscaling.MetricsCollector;
import io.pixelsdb.pixels.turbo.planner.queue.QueryQueues;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.properties.PixelsSessionProperties;
import io.pixelsdb.pixels.trino.properties.PixelsTableProperties;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class PixelsConnector implements Connector
{
    private static final Logger logger = Logger.get(PixelsConnector.class);

    private final PixelsConnectorId connectorId;
    private final LifeCycleManager lifeCycleManager;
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsSplitManager splitManager;
    private final boolean recordCursorEnabled;
    private final PixelsPageSourceProvider pageSourceProvider;
    private final PixelsRecordSetProvider recordSetProvider;
    private final PixelsSessionProperties sessionProperties;
    private final PixelsTableProperties tableProperties;
    private final PixelsTrinoConfig config;
    private final TransService transService;

    @Inject
    public PixelsConnector(
            PixelsConnectorId connectorId,
            LifeCycleManager lifeCycleManager,
            PixelsMetadataProxy metadataProxy,
            PixelsSplitManager splitManager,
            PixelsTrinoConfig config,
            PixelsPageSourceProvider pageSourceProvider,
            PixelsRecordSetProvider recordSetProvider,
            PixelsSessionProperties sessionProperties,
            PixelsTableProperties tableProperties)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "recordSetProvider is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.config = requireNonNull(config, "config is null");
        requireNonNull(config, "config is null");
        this.recordCursorEnabled = Boolean.parseBoolean(config.getConfigFactory().getProperty("record.cursor.enabled"));
        this.transService = new TransService(config.getConfigFactory().getProperty("trans.server.host"),
                Integer.parseInt(config.getConfigFactory().getProperty("trans.server.port")));
        if (this.config.getLambdaSwitch() == PixelsTrinoConfig.LambdaSwitch.AUTO)
        {
            MetricsCollector.Instance().startAutoReport();
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                                                       boolean readOnly, boolean autoCommit)
    {
        /**
         * PIXELS-172:
         * Be careful that Presto does not set readOnly to true for normal queries.
         */
        QueryTransInfo info;
        try
        {
            info = this.transService.getQueryTransInfo();
        } catch (TransException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_TRANS_SERVICE_ERROR, e);
        }
        TransContext.Instance().beginQuery(info);
        QueryQueues.ExecutorType executorType;
        if (this.config.getLambdaSwitch() == PixelsTrinoConfig.LambdaSwitch.AUTO)
        {
            MetricsCollector.Instance().report();
            while ((executorType = QueryQueues.Instance().Enqueue(info.getQueryId())) == QueryQueues.ExecutorType.None)
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e)
                {
                    logger.error("Interrupted while waiting for enqueue the query id.");
                }
            }
        }
        else if (config.getLambdaSwitch() == PixelsTrinoConfig.LambdaSwitch.ON)
        {
            executorType = QueryQueues.ExecutorType.Lambda;
        }
        else
        {
            executorType = QueryQueues.ExecutorType.Cluster;
        }
        return new PixelsTransactionHandle(info.getQueryId(), info.getQueryTimestamp(), executorType);
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        if (transactionHandle instanceof PixelsTransactionHandle)
        {
            PixelsTransactionHandle handle = (PixelsTransactionHandle) transactionHandle;
            QueryQueues.Instance().Dequeue(handle.getTransId(), handle.getExecutorType());
            TransContext.Instance().commitQuery(handle.getTransId());
            cleanIntermediatePathForQuery(handle.getTransId());
        } else
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_TRANS_HANDLE_TYPE_ERROR,
                    "The transaction handle is not an instance of PixelsTransactionHandle.");
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        if (transactionHandle instanceof PixelsTransactionHandle)
        {
            PixelsTransactionHandle handle = (PixelsTransactionHandle) transactionHandle;
            QueryQueues.Instance().Dequeue(handle.getTransId(), handle.getExecutorType());
            TransContext.Instance().rollbackQuery(handle.getTransId());
            if (handle.getExecutorType() == QueryQueues.ExecutorType.Lambda)
            {
                cleanIntermediatePathForQuery(handle.getTransId());
            }
        } else
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_TRANS_HANDLE_TYPE_ERROR,
                    "The transaction handle is not an instance of PixelsTransactionHandle.");
        }
    }

    private void cleanIntermediatePathForQuery(long queryId)
    {
        try
        {
            if (config.isCleanLocalResult())
            {
                IntermediateFileCleaner.Instance().asyncDelete(config.getOutputFolderForQuery(queryId));
            }
        } catch (InterruptedException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    "Failed to clean intermediate path for the query");
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transHandle)
    {
        return new PixelsMetadata(connectorId, metadataProxy, config, (PixelsTransactionHandle) transHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    @Override
    public PixelsPageSourceProvider getPageSourceProvider() {
        if (this.recordCursorEnabled)
        {
            throw new UnsupportedOperationException();
        }
        return pageSourceProvider;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        if (this.recordCursorEnabled)
        {
            return recordSetProvider;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
            MetricsCollector.Instance().stopAutoReport();
        } catch (Exception e) {
            logger.error(e, "error in shutting down connector");
            throw new TrinoException(PixelsErrorCode.PIXELS_CONNECTOR_ERROR, e);
        }
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }
}
