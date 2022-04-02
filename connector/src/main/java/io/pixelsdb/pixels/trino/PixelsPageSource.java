package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;

public class PixelsPageSource implements ConnectorPageSource {
    public PixelsPageSource(PixelsSplit pixelsSplit, List<PixelsColumnHandle> pixelsColumns, Storage storage, MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache pixelsFooterCache, String connectorId) {
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public Page getNextPage() {
        return null;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
