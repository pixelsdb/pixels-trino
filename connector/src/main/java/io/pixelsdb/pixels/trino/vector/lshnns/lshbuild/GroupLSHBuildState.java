package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

/**
 * State class used while calling the udf exactNNS() with group by. probably not very useful for now, but is required
 * by trino aggregation framework if we implement our own state factory, which we did.
 */
public class GroupLSHBuildState implements LSHBuildState {

    @Override
    public void init(int dimension, int numPlanes) {

    }

    @Override
    public HashMap<BitSet, ArrayList<double[]>> getBuckets() {
        return null;
    }

    @Override
    public void combineWithOtherState(LSHBuildState otherState) {

    }

    @Override
    public void updateBuckets(double[] vec) {

    }

    @Override
    public int getDimension() {
        return 0;
    }

    @Override
    public int getNumBits() {
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
    public LSHFunc getLshFunc() {
        return null;
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

    @Override
    public void clearBuckets() {

    }
}
