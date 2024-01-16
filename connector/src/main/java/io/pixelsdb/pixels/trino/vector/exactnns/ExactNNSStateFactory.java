package io.pixelsdb.pixels.trino.vector.exactnns;

import io.trino.spi.function.AccumulatorStateFactory;

public class ExactNNSStateFactory implements AccumulatorStateFactory<ExactNNSState> {

    public ExactNNSStateFactory() {

    }
    ;
    @Override
    public ExactNNSState createSingleState() {
        return new SingleExactNNSState();
    }

    @Override
    public ExactNNSState createGroupedState() {
        return new GroupExactNNSState();
    }
}
