package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

@AccumulatorStateMetadata(
        stateFactoryClass = LSHNNSStateFactory.class,
        stateSerializerClass = LSHNNSStateSerializer.class,
        serializedType = "ARRAY(DOUBLE)")
public interface LSHBuildState
        extends AccumulatorState
{
    //todo I think each state having a priority queue should work
    //todo also I think I need to implement a state factory

    // each state maintains a priority queue, in the input function we initialize the priority queue;
    // in the combine function, we merge the priority queue from the other state to state
    // in the output function we turn the priority queue into a json

    // todo maybe this should be in the constructor and be in the factory instead
    // but this will probably work as well
    void init(int dimension, int numPlanes);

    HashMap<BitSet, ArrayList<double[]>> getBuckets();

    void combineWithOtherState(LSHBuildState otherState);

    void updateBuckets(double[] vec);

    int getDimension();

    int getNumBits();

    void serialize(BlockBuilder out);

    void deserialize(Block block);

    LSHFunc getLshFunc();

    void clearBuckets();
}
