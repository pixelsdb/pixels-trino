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

import com.alibaba.fastjson.parser.ParserConfig;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.trino.exception.ListenerException;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static io.pixelsdb.pixels.trino.exception.ListenerErrorCode.PIXELS_EVENT_LISTENER_ERROR;

/**
 * @author hank
 */
public class PixelsEventListener implements EventListener
{
    private static final Logger logger = Logger.get(PixelsEventListener.class);

    private final String logDir;
    private final boolean enabled;
    private final String userPrefix;
    private final String schema;
    private final String queryType;
    private static BufferedWriter LogWriter = null;
    private static final double GCThreshold;

    static
    {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
        /**
         * Issue #87:
         * Get the gc threshold here.
         */
        String thresholdStr = ConfigFactory.Instance().getProperty("experimental.gc.threshold");
        GCThreshold = Double.parseDouble(thresholdStr);
        logger.info("Using experimental.gc.threshold (" + GCThreshold + ")...");
    }

    public PixelsEventListener (String logDir, boolean enabled,
                                String userPrefix,
                                String schema,
                                String queryType)
    {
        this.logDir = logDir.endsWith("/") ? logDir : logDir + "/";
        this.enabled = enabled;
        this.userPrefix = userPrefix;
        this.schema = schema;
        this.queryType = queryType;
        try
        {
            if (this.enabled && LogWriter == null)
            {
                LogWriter = new BufferedWriter(new FileWriter(
                                this.logDir + "pixels_trino_query" +
                                        DateUtil.getCurTime() + ".log", true));
                LogWriter.write("\"query id\",\"user\",\"wall(ms)\",\"rs waiting(ms)\",\"queued(ms)\"," +
                        "\"planning(ms)\",\"execution(ms)\",\"read throughput(MB)\",\"local gc time(ms)\"," +
                        "\"full gc tasks\",\"avg full gc time(s)\"");
                LogWriter.newLine();
                LogWriter.flush();
            }
        } catch (IOException e)
        {
            throw new TrinoException(PIXELS_EVENT_LISTENER_ERROR,
                    new ListenerException("can not create log writer."));
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (!this.enabled)
        {
            return;
        }

        double free = Runtime.getRuntime().freeMemory();
        double total = Runtime.getRuntime().totalMemory();

        /**
         * Issue #87:
         * Do explicit gc here, instead of in PixelsReaderImpl.close().
         */
        long gcms = -1;
        if (free / total < GCThreshold)
        {
            /**
             * By calling gc(), we try to do gc on time when the query is finished.
             * It would be very expensive to do gc when executing small queries.
             */
            long start = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            gcms = (System.currentTimeMillis() - start);
            logger.info("GC time after query: " + gcms + " ms.");
        }

        /**
         * Issue #132:
         * TODO: add cpu and memory statistics to the output.
         * TODO: make use of resource estimates.
         */
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        String user = queryCompletedEvent.getContext().getUser();
        if (queryCompletedEvent.getContext().getSchema().isEmpty())
        {
            logger.error("can not write log in pixels event listener.");
            logger.info("query id: " + queryId + ", user: " + user);
            return;
        }
        String schema = queryCompletedEvent.getContext().getSchema().get();
        if (schema.equalsIgnoreCase(this.schema))
        {
            if (this.userPrefix.equals("none") || user.startsWith(this.userPrefix))
            {
                try
                {
                    String query = queryCompletedEvent.getMetadata().getQuery();
                    if (query.toLowerCase().contains(this.queryType.toLowerCase()))
                    {
                        QueryStatistics stats = queryCompletedEvent.getStatistics();
                        long execution = -1;
                        if (stats.getExecutionTime().isPresent())
                        {
                            execution = stats.getExecutionTime().get().toMillis();
                        }
                        long queued = stats.getQueuedTime().toMillis();
                        long planning = -1;
                        if (stats.getPlanningTime().isPresent())
                        {
                            planning = stats.getPlanningTime().get().toMillis();
                        }
                        long rsWaiting = -1;
                        if (stats.getResourceWaitingTime().isPresent())
                        {
                            rsWaiting = stats.getResourceWaitingTime().get().toMillis();
                        }
                        long wall = stats.getWallTime().toMillis();
                        double inputDataSize = stats.getPhysicalInputBytes();
                        double throughput = inputDataSize / execution / 1024;

                        double totalGcSec = 0;
                        int gcTasks = 0;
                        int tasks = 0;
                        List<StageGcStatistics> stageGcStats = stats.getStageGcStatistics();
                        for (StageGcStatistics gcStats : stageGcStats)
                        {
                            totalGcSec += gcStats.getTotalFullGcSec();
                            gcTasks += gcStats.getFullGcTasks();
                            tasks += gcStats.getTasks();
                        }

                        LogWriter.write(queryId + "," + user + "," + wall + "," + rsWaiting + "," +
                                queued + "," + planning + "," + execution + "," + throughput + "," +
                                (gcms>=0 ? gcms : "na") + "," + gcTasks + "," + (tasks > 0 ? totalGcSec/tasks : "na"));
                        LogWriter.newLine();
                        LogWriter.flush();
                    }
                } catch (IOException e)
                {
                    logger.error("can not write log in pixels event listener.");
                    logger.info("query id: " + queryId + ", user: " + user);
                }
            }
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
