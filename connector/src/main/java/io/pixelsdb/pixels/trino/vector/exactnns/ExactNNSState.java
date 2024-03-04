package io.pixelsdb.pixels.trino.vector.exactnns;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.trino.vector.VectorDistFunc;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

import java.util.PriorityQueue;

@AccumulatorStateMetadata(
        stateFactoryClass = ExactNNSStateFactory.class,
        stateSerializerClass = ExactNNSStateSerializer.class,
        serializedType = "ARRAY(DOUBLE)")
public interface ExactNNSState
        extends AccumulatorState
{
    //todo I think each state having a priority queue should work
    //todo also I think I need to implement a state factory

    // each state maintains a priority queue, in the input function we initialize the priority queue;
    // in the combine function, we merge the priority queue from the other state to state
    // in the output function we turn the priority queue into a json

    // todo maybe this should be in the constructor and be in the factory instead
    // but this will probably work as well
    void init(double[] inputVec,
                     int dimension,
                     Slice distFuncSlice,
                     int k);

    PriorityQueue<double[]> getNearestVecs();

    void combineWithOtherState(ExactNNSState otherState);

    void updateNearestVecs(double[] vec);

    int getDimension();

    int getK();

    public double[] getInputVec();

    public VectorDistFuncs.DistFuncEnum getVectorDistFuncEnum();

    public SingleExactNNSState.VecDistComparator getVecDistDescComparator();

    void serialize(BlockBuilder out);

    void deserialize(Block block);
}
