package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.trino.spi.function.AccumulatorStateFactory;

public class LSHNNSStateFactory implements AccumulatorStateFactory<LSHBuildState> {

    public LSHNNSStateFactory() {

    }
    ;
    @Override
    public LSHBuildState createSingleState() {
        return new SingleLSHBuildState();
    }

    @Override
    public LSHBuildState createGroupedState() {
        return new GroupLSHBuildState();
    }
}
