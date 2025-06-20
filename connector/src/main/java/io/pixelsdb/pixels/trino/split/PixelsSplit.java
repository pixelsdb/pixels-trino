package io.pixelsdb.pixels.trino.split;

import io.trino.spi.connector.ConnectorSplit;

public interface PixelsSplit extends ConnectorSplit {
    public enum PixelsSplitType {
        FILE,
        BUFFER
    }
    
    public String getConnectorId();
    public String getStorageScheme();
}
