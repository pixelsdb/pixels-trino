package io.pixelsdb.pixels.trino.vector.exactnns;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.trino.vector.VectorDistFunc;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.util.PriorityQueue;

/**
 * State class used while calling the udf exactNNS() with group by. probably not very useful for now, but is required
 * by trino aggregation framework if we implement our own state factory, which we did.
 */
public class GroupExactNNSState implements ExactNNSState{

    @Override
    public void init(double[] inputVec,
                     int dimension,
                     Slice distFuncSlice,
                     int k) {

    }

    @Override
    public PriorityQueue<double[]> getNearestVecs() {
        return null;
    }

    @Override
    public void combineWithOtherState(ExactNNSState otherState) {

    }

    @Override
    public void updateNearestVecs(double[] vec) {

    }

    @Override
    public int getDimension() {
        return 0;
    }

    @Override
    public int getK() {
        return 0;
    }

    @Override
    public void
    serialize(BlockBuilder out) {

    }

    @Override
    public void deserialize(Block block) {

    }

    @Override
    public long getEstimatedSize() {
        return 0;
    }

    public double[] getInputVec() {
        return null;
    }

    public VectorDistFuncs.DistFuncEnum getVectorDistFuncEnum() {
        return null;
    }

    public SingleExactNNSState.VecDistComparator getVecDistDescComparator() {
        return null;
    }
}
