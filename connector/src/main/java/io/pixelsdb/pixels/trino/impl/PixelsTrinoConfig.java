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
package io.pixelsdb.pixels.trino.impl;

import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.MetricsCollector;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.PixelsPlanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfoBuilder;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Optional;

import static io.pixelsdb.pixels.common.utils.Constants.CF_OUTPUT_STATE_KEY_PREFIX;

/**
 * The configuration read from etc/catalog/pixels.properties.
 *
 * @author tao
 * @author hank
 **/
public class PixelsTrinoConfig
{
    private final Logger logger = Logger.get(PixelsTrinoConfig.class);
    private ConfigFactory configFactory = null;

    private static int BatchSize = 1000;

    public static int getBatchSize()
    {
        return BatchSize;
    }

    public enum CloudFunctionSwitch { ON, OFF, AUTO, SESSION }

    private CloudFunctionSwitch cloudFunctionSwitch = CloudFunctionSwitch.AUTO;
    private boolean cleanIntermediateResult = true;
    /**
     * The storage info of the inputs of Pixels Turbo.
     */
    private StorageInfo inputStorageInfo = null;
    private Storage.Scheme inputStorageScheme = null;
    /**
     * The storage info of the outputs of Pixels Turbo.
     */
    private StorageInfo outputStorageInfo = null;
    private Storage.Scheme outputStorageScheme = null;
    private String outputFolder = null;

    @Config("cloud.function.switch")
    public PixelsTrinoConfig setCloudFunctionSwitch(String cloudFunctionSwitch)
    {
        this.cloudFunctionSwitch = CloudFunctionSwitch.valueOf(cloudFunctionSwitch.toUpperCase());

        // Issue #87: remove pixels.config and move configuration reloading to here.
        String pixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
        if (pixelsHome == null)
        {
            logger.info("using pixels.properties in jar.");
        } else
        {
            logger.info("using pixels.properties in pixels.home: " + pixelsHome);
        }

        this.configFactory = ConfigFactory.Instance();
        try
        {
            int batchSize = Integer.parseInt(this.configFactory.getProperty("row.batch.size"));
            if (batchSize > 0)
            {
                BatchSize = batchSize;
                logger.info("using pixels row.batch.size: " + BatchSize);
            }
        } catch (NumberFormatException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
        }
        try
        {
            /**
             * PIXELS-108:
             * We reload the storage here, because in other classes like
             * PixelsSplitManager, when we try to create storage instance
             * by StorageFactory.Instance().getStorage(), Presto does not
             * load the dependencies (e.g. hadoop-hdfs-xxx.jar) of the storage
             * implementation (e.g. io.pixelsdb.pixels.common.physical.storage.HDFS).
             *
             * I currently don't know the reason (08.27.2021).
             */
            if (StorageFactory.Instance().isEnabled(Storage.Scheme.hdfs))
            {
                // PIXELS-385: only reload HDFS if it is enabled.
                StorageFactory.Instance().reload(Storage.Scheme.hdfs);
            }
        } catch (IOException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }

        if (this.cloudFunctionSwitch == CloudFunctionSwitch.ON ||
                this.cloudFunctionSwitch == CloudFunctionSwitch.AUTO ||
                this.cloudFunctionSwitch == CloudFunctionSwitch.SESSION)
        {
            /**
             * PIXELS-416:
             * We must load the invoker providers here. The split manager and the page source
             * provider are running in working threads and can not load the invoker providers
             * successfully. The detailed reason is to be analyzed.
             */
            InvokerFactory.Instance();
            if (this.cloudFunctionSwitch == CloudFunctionSwitch.AUTO ||
                    this.cloudFunctionSwitch == CloudFunctionSwitch.SESSION)
            {
                // PIXELS-416: same as the invoker providers.
                MetricsCollector.Instance();
            }

            // Issue #81: only init input and output storage schemes when serverless workers are enabled.
            this.inputStorageScheme = Storage.Scheme.from(this.configFactory.getProperty("executor.input.storage.scheme"));
            this.inputStorageInfo = StorageInfoBuilder.BuildFromConfig(this.inputStorageScheme);

            this.outputStorageScheme = Storage.Scheme.from(this.configFactory.getProperty("executor.output.storage.scheme"));
            this.outputStorageInfo = StorageInfoBuilder.BuildFromConfig(this.outputStorageScheme);
            this.outputFolder = this.configFactory.getProperty("executor.output.folder");
            if (!this.outputFolder.endsWith("/"))
            {
                this.outputFolder += "/";
            }
        }
        return this;
    }

    @Config("clean.intermediate.result")
    public PixelsTrinoConfig setCleanIntermediateResult(boolean cleanIntermediateResult)
    {
        this.cleanIntermediateResult = cleanIntermediateResult;
        return this;
    }

    @NotNull
    public PixelsTrinoConfig.CloudFunctionSwitch getCloudFunctionSwitch()
    {
        return cloudFunctionSwitch;
    }

    public boolean isCleanIntermediateResult()
    {
        return cleanIntermediateResult;
    }

    public StorageInfo getInputStorageInfo()
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use input storage when cloud function is turned off"));
        }
        return inputStorageInfo;
    }

    public Storage.Scheme getInputStorageScheme()
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use input storage when cloud function is turned off"));
        }
        return inputStorageScheme;
    }

    public StorageInfo getOutputStorageInfo()
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use output storage when cloud function is turned off"));
        }
        return outputStorageInfo;
    }

    public Storage.Scheme getOutputStorageScheme()
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use output storage when cloud function is turned off"));
        }
        return outputStorageScheme;
    }

    /**
     * Get the output folder path for a transaction (query). It will end with
     * a slash '/', hence there is no need to add a slash manually.
     * @param transId the transaction id of the query
     * @return the output folder, it has a slash '/' at the end
     */
    public String getOutputFolderForQuery(long transId)
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use output storage when cloud function is turned off"));
        }
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.outputFolder + transId + "/";
    }

    /**
     * Get the output folder path for a transaction (query). It will end with
     * a slash '/', hence there is no need to add a slash manually.
     * @param transId the transaction id of the query
     * @param post the postfix to be appended to the end of the folder path
     * @return the output folder, it has a slash '/' at the end
     */
    public String getOutputFolderForQuery(long transId, String post)
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use output storage when cloud function is turned off"));
        }
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        if (!post.endsWith("/"))
        {
            post += "/";
        }
        return this.outputFolder + transId + (post.startsWith("/") ? "" : "/") + post;
    }

    /**
     * Get the output file path for a transaction (query). It will not end with a slash '/'.
     * @param transId the transaction id of the query
     * @param post the postfix to be appended to the end of the file path
     * @return the output file path
     */
    public String getOutputFilePathForQuery(long transId, String post)
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use output storage when cloud function is turned off"));
        }
        if (post.endsWith("/"))
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    "the file path postfix should not end with as slash '/'");
        }
        return this.outputFolder + transId + (post.startsWith("/") ? "" : "/") + post;
    }

    /**
     * Get the intermediate folder for a transaction (query). It will end with
     * a slash '/', hence there is no need to add a slash manually.
     * @param transId the transaction id of the query
     * @return the output folder, it has a slash '/' at the end
     */
    public String getIntermediateFolderForQuery(long transId)
    {
        if (this.cloudFunctionSwitch == CloudFunctionSwitch.OFF)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR,
                    new Throwable("should not use intermediate storage when cloud function is turned off"));
        }
        return PixelsPlanner.getIntermediateFolderForTrans(transId);
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return the configuration factory instance
     */
    @NotNull
    public ConfigFactory getConfigFactory()
    {
        return this.configFactory;
    }

    /**
     * Generate the prefix of the output state key for a serverless worker.
     * @param transId the transaction id of the query
     * @param schemaTableName the 'schema.table' of the root table of the sub-plan executed in serverless workers
     * @return
     */
    public static String getOutputStateKeyPrefix(long transId, Optional<SchemaTableName> schemaTableName)
    {
        String prefix = CF_OUTPUT_STATE_KEY_PREFIX + "_" + transId + "_";
        if (schemaTableName.isPresent())
        {
            prefix += schemaTableName.get() + "_";
        }
        return prefix;
    }
}
