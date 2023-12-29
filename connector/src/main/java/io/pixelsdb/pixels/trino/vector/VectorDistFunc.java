package io.pixelsdb.pixels.trino.vector;

import io.trino.spi.block.Block;

public interface VectorDistFunc {
    Double getDist(double[] vec1, double[] vec2);
}
