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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
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

    private String pixelsConfig = null;
    private boolean lambdaEnabled = false;
    private boolean cleanLocalResult = true;
    private int localScanConcurrency = -1;
    private Storage.Scheme outputScheme = null;
    private String outputFolder = null;
    private String outputEndpoint = null;
    private String outputAccessKey = null;
    private String outputSecretKey = null;

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
            //try
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
                //StorageFactory.Instance().reload();
            } //catch (IOException e)
            {
                //throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
            }
        }
        return this;
    }

    @Config("lambda.enabled")
    public PixelsTrinoConfig setLambdaEnabled(boolean enabled)
    {
        this.lambdaEnabled = enabled;
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

    @Config("output.scheme")
    public PixelsTrinoConfig setOutputScheme(String outputScheme)
    {
        this.outputScheme = Storage.Scheme.from(outputScheme);
        return this;
    }

    @Config("output.folder")
    public PixelsTrinoConfig setOutputFolder(String folder)
    {
        if (!folder.endsWith("/"))
        {
            folder = folder + "/";
        }
        this.outputFolder = folder;
        return this;
    }

    @Config("output.access.key")
    public PixelsTrinoConfig setOutputAccessKey(String accessKey)
    {
        this.outputAccessKey = accessKey;
        return this;
    }

    @Config("output.secret.key")
    public PixelsTrinoConfig setOutputSecretKey(String secretKey)
    {
        this.outputSecretKey = secretKey;
        return this;
    }

    @Config("output.endpoint")
    public PixelsTrinoConfig setOutputEndpoint(String endpoint)
    {
        this.outputEndpoint = endpoint;
        return this;
    }

    @NotNull
    public String getPixelsConfig ()
    {
        return this.pixelsConfig;
    }

    public boolean isLambdaEnabled()
    {
        return lambdaEnabled;
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
    public Storage.Scheme getOutputScheme()
    {
        return outputScheme;
    }

    @NotNull
    public String getOutputFolder()
    {
        return outputFolder;
    }

    @NotNull
    public String getOutputFolderForQuery(long queryId)
    {
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.outputFolder + queryId + "/";
    }

    @NotNull
    public String getOutputFolderForQuery(long queryId, String post)
    {
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.outputFolder + queryId + "/" +post + "/";
    }

    @NotNull
    public String getOutputAccessKey()
    {
        return outputAccessKey;
    }

    @NotNull
    public String getOutputSecretKey()
    {
        return outputSecretKey;
    }

    @NotNull
    public String getOutputEndpoint()
    {
        return outputEndpoint;
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
