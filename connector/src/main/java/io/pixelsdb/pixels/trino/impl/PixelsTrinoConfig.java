package io.pixelsdb.pixels.trino.impl;

import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * @Description: Configuration read from etc/catalog/pixels.properties
 * @author: tao
 * @date: Create in 2018-01-20 11:16
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

    private String pixelsHome = null;

    @Config("pixels.home")
    public PixelsTrinoConfig setPixelsHome (String pixelsHome)
    {
        this.pixelsHome = pixelsHome;

        // reload configuration
        if (this.configFactory == null)
        {
            if (pixelsHome == null || pixelsHome.isEmpty())
            {
                String defaultPixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
                if (defaultPixelsHome == null)
                {
                    logger.info("using pixels.properties insided in jar.");
                } else
                {
                    logger.info("using pixels.properties under default pixels.home: " + defaultPixelsHome);
                }
            } else
            {
                if (!(pixelsHome.endsWith("/") || pixelsHome.endsWith("\\")))
                {
                    pixelsHome += "/";
                }
                try
                {
                    ConfigFactory.Instance().loadProperties(pixelsHome + "pixels.properties");
                    ConfigFactory.Instance().addProperty("pixels.home", pixelsHome);
                    logger.info("using pixels.properties under connector specified pixels.home: " + pixelsHome);

                } catch (IOException e)
                {
                    logger.error(e,"can not load pixels.properties under: " + pixelsHome +
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
                StorageFactory.Instance().reload();
            } catch (IOException e)
            {
                throw new TrinoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
            }
        }
        return this;
    }

    @NotNull
    public String getPixelsHome ()
    {
        return this.pixelsHome;
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
