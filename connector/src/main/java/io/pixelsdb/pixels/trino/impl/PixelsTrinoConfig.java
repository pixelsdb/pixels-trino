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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
    private int localScanConcurrency = -1;
    private String minioOutputFolder = null;
    private String minioEndpointIP = null;
    private int minioEndpointPort = -1;
    private String minioEndpoint = null;
    private String minioAccessKey = null;
    private String minioSecretKey = null;

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
        if (enabled)
        {
            this.minioEndpointIP = EC2MetadataUtils.getInstanceInfo().getPrivateIp();
        }
        else
        {
            try
            {
                this.minioEndpointIP = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e)
            {
                logger.error(e, "failed to get local ip address when lambda is disabled");
                this.minioEndpointIP = "127.0.0.1";
            }
        }
        logger.info("using minio endpoint ip address: " + this.minioEndpointIP);
        return this;
    }

    @Config("local.scan.concurrency")
    public PixelsTrinoConfig setLocalScanConcurrency(int concurrency)
    {
        this.localScanConcurrency = concurrency;
        return this;
    }

    @Config("minio.output.folder")
    public PixelsTrinoConfig setMinioOutputFolder(String folder)
    {
        if (!folder.endsWith("/"))
        {
            folder = folder + "/";
        }
        this.minioOutputFolder = folder;
        return this;
    }

    @Config("minio.endpoint.port")
    public PixelsTrinoConfig setMinioEndpointPort(int port)
    {
        this.minioEndpointPort = port;
        return this;
    }

    @Config("minio.access.key")
    public PixelsTrinoConfig setMinioAccessKey(String accessKey)
    {
        this.minioAccessKey = accessKey;
        return this;
    }

    @Config("minio.secret.key")
    public PixelsTrinoConfig setMinioSecretKey(String secretKey)
    {
        this.minioSecretKey = secretKey;
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

    @NotNull
    public String getMinioOutputFolder()
    {
        return minioOutputFolder;
    }

    @NotNull
    public String getMinioOutputFolderForQuery(long queryId)
    {
        /* Must end with '/', otherwise it will not be considered
         * as a folder in S3-like storage.
         */
        return this.minioOutputFolder + queryId + "/";
    }

    @NotNull
    public String getMinioEndpointIP()
    {
        return minioEndpointIP;
    }

    public int getMinioEndpointPort()
    {
        return minioEndpointPort;
    }

    @NotNull
    public String getMinioAccessKey()
    {
        return minioAccessKey;
    }

    @NotNull
    public String getMinioSecretKey()
    {
        return minioSecretKey;
    }

    @NotNull
    public String getMinioEndpoint()
    {
        if (this.minioEndpoint == null)
        {
            this.minioEndpoint = "http://" + this.minioEndpointIP + ":" + minioEndpointPort;
        }
        return minioEndpoint;
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
