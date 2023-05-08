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
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * The configuration read from etc/catalog/pixels.properties.
 *
 * @author tao
 * @author hank
 **/
public class PixelsTrinoConfig
{
    private Logger logger = Logger.get(PixelsTrinoConfig.class);
    private ConfigFactory configFactory = null;

    private static int BatchSize = 1000;

    public static int getBatchSize()
    {
        return BatchSize;
    }

    public enum LambdaSwitch { ON, OFF, AUTO }

    private String pixelsConfig = null;
    private LambdaSwitch lambdaSwitch = LambdaSwitch.AUTO;
    private boolean cleanLocalResult = true;
    private int localScanConcurrency = -1;
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

    @Config("pixels.config")
    public PixelsTrinoConfig setPixelsConfig (String pixelsConfig)
    {
        this.pixelsConfig = pixelsConfig;

        // reload configuration
        if (this.configFactory == null)
        {
            if (pixelsConfig == null || pixelsConfig.isEmpty())
            {
                String pixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
                if (pixelsHome == null)
                {
                    logger.info("using pixels.properties in jar.");
                } else
                {
                    logger.info("using pixels.properties under default pixels.home: " + pixelsHome);
                }
            } else
            {
                try
                {
                    ConfigFactory.Instance().loadProperties(pixelsConfig);
                    logger.info("using pixels.properties specified by the connector: " + pixelsConfig);

                } catch (IOException e)
                {
                    logger.error(e,"can not load pixels.properties: " + pixelsConfig +
                            ", configuration reloading is skipped.");
                    throw new TrinoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
                }
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
        }

        this.inputStorageScheme = Storage.Scheme.from(this.configFactory.getProperty("executor.input.storage.scheme"));
        String inputEndpoint = this.configFactory.getProperty("executor.input.storage.endpoint");
        String inputAccessKey = this.configFactory.getProperty("executor.input.storage.access.key");
        String inputSecretKey = this.configFactory.getProperty("executor.input.storage.secret.key");
        this.inputStorageInfo = new StorageInfo(this.inputStorageScheme, inputEndpoint, inputAccessKey, inputSecretKey);

        this.outputStorageScheme = Storage.Scheme.from(this.configFactory.getProperty("executor.output.storage.scheme"));
        String outputEndpoint = this.configFactory.getProperty("executor.output.storage.endpoint");
        String outputAccessKey = this.configFactory.getProperty("executor.output.storage.access.key");
        String outputSecretKey = this.configFactory.getProperty("executor.output.storage.secret.key");
        this.outputStorageInfo = new StorageInfo(this.outputStorageScheme, outputEndpoint, outputAccessKey, outputSecretKey);
        this.outputFolder = this.configFactory.getProperty("executor.output.folder");
        if (!this.outputFolder.endsWith("/"))
        {
            this.outputFolder += "/";
        }

        return this;
    }

    @Config("lambda.switch")
    public PixelsTrinoConfig setLambdaSwitch(String lambdaSwitch)
    {
        this.lambdaSwitch = LambdaSwitch.valueOf(lambdaSwitch.toUpperCase());
        if (this.lambdaSwitch == LambdaSwitch.ON || this.lambdaSwitch == LambdaSwitch.AUTO)
        {
            /**
             * PIXELS-416:
             * We must load the invoker providers here. The split manager and the page source
             * provider are running in working threads and can not load the invoker providers
             * successfully. The detailed reason is to be analyzed.
             */
            InvokerFactory.Instance();
            if (this.lambdaSwitch == LambdaSwitch.AUTO)
            {
                // PIXELS-416: same as the invoker providers.
                MetricsCollector.Instance();
            }
        }
        return this;
    }

    @Config("clean.local.result")
    public PixelsTrinoConfig setCleanLocalResult(boolean cleanLocalResult)
    {
        this.cleanLocalResult = cleanLocalResult;
        return this;
    }

    @Config("local.scan.concurrency")
    public PixelsTrinoConfig setLocalScanConcurrency(int concurrency)
    {
        this.localScanConcurrency = concurrency;
        return this;
    }

    @NotNull
    public String getPixelsConfig ()
    {
        return this.pixelsConfig;
    }

    @NotNull
    public LambdaSwitch getLambdaSwitch()
    {
        return lambdaSwitch;
    }

    public int getLocalScanConcurrency()
    {
        return localScanConcurrency;
    }

    public boolean isCleanLocalResult()
    {
        return cleanLocalResult;
    }

    @NotNull
    public StorageInfo getInputStorageInfo()
    {
        return inputStorageInfo;
    }

    @NotNull
    public Storage.Scheme getInputStorageScheme()
    {
        return inputStorageScheme;
    }

    @NotNull
    public StorageInfo getOutputStorageInfo()
    {
        return outputStorageInfo;
    }

    @NotNull
    public Storage.Scheme getOutputStorageScheme()
    {
        return outputStorageScheme;
    }

    @NotNull
    public String getOutputFolderForQuery(long transId)
    {
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.outputFolder + transId + "/";
    }

    @NotNull
    public String getOutputFolderForQuery(long transId, String post)
    {
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.outputFolder + transId + "/" +post + "/";
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return
     */
    @NotNull
    public ConfigFactory getConfigFactory()
    {
        return this.configFactory;
    }
}
