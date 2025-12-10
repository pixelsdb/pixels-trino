/*
 * Copyright 2025 PixelsDB.
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

import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Detector for long-running queries that need to be offloaded to disk checkpoints.
 * This ensures that long-running queries do not block garbage collection by
 * creating checkpoints and pushing watermarks.
 * 
 * @author gengdy
 * @create 2025-12-09
 */
public class PixelsOffloadDetector
{
    private static final Logger logger = Logger.get(PixelsOffloadDetector.class);

    private final RetinaService retinaService;
    private final TransService transService;
    private final long offloadThreshold;
    private final ScheduledExecutorService scheduler;
    private final Map<Long, TransContext> activeQueries;

    public PixelsOffloadDetector()
    {
        this.offloadThreshold = 1000 * Long.parseLong(ConfigFactory.Instance().getProperty("pixels.transaction.offload.threshold"));
        this.activeQueries = new ConcurrentHashMap<>();
        this.retinaService = RetinaService.Instance();
        this.transService = TransService.Instance();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pixels-offload-detector");
            t.setDaemon(true);
            return t;
        });

        long checkInterval = 5;
        this.scheduler.scheduleAtFixedRate(
                this::detectAndOffload, checkInterval, checkInterval, TimeUnit.SECONDS);
        
        logger.info("PixelsOffloadDetector started with threshold=%d s, check interval=%d s",
                offloadThreshold, checkInterval);
    }

    /**
     * Register a query for long-running detection.
     * 
     * @param context
     */
    public void registerQuery(TransContext context)
    {
        this.activeQueries.put(context.getTransId(), context);
    }

    /**
     * Unregister a query when it completes.
     * If the query was offloaded, this will call unregisterOffload to clean up the checkpoint.
     * 
     * @param transId the transaction id
     */
    public void unregisterQuery(long transId)
    {
        TransContext context = this.activeQueries.remove(transId);
        if (context != null && context.isOffloaded())
        {
            try
            {
                this.retinaService.unregisterOffload(context.getTimestamp());
            } catch (RetinaException e)
            {
                logger.error("Failed to unregister offload for timestamp={}", context.getTimestamp(), e);
            }
        }
    }

    /**
     * Periodically detect and offload long-running queries.
     */
    private void detectAndOffload()
    {
        long now = System.currentTimeMillis();
        for (TransContext context : activeQueries.values())
        {
            // Skip if already offloaded
            if (context.isOffloaded())
            {
                continue;
            }

            long runningTime = now - context.getStartTime();
            if (runningTime > offloadThreshold)
            {
                try
                {
                    // 1. Register offload with Retina - this creates the checkpoint
                    this.retinaService.registerOffload(context.getTimestamp());
                    
                    // 2. Notify TransService to mark the transaction as offloaded on daemon side
                    this.transService.markTransOffloaded(context.getTransId());
                    
                    // 3. Mark as offloaded locally
                    context.setOffloaded(true);
                    
                    logger.info("Offloaded long-running query: transId=%d, running time=%d s",
                            context.getTransId(), runningTime/1000);
                } catch (RetinaException e)
                {
                    logger.error("Failed to register offload for timestamp={}", context.getTimestamp(), e);
                } catch (TransException e)
                {
                    logger.error(e, "Failed to mark transaction as offloaded or push watermark for transId=%d", context.getTransId());
                }
            }
        }
    }

    /**
     * Shutdown the detector and clean up resources.
     */
    public void shutdown()
    {
        this.scheduler.shutdownNow();
        try
        {
            this.scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            logger.error("Interrupted while waiting for offload detector to shutdown");
            Thread.currentThread().interrupt();
        }
    }
}
