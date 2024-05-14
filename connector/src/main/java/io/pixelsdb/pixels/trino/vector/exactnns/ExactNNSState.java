/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino.vector.exactnns;

import io.airlift.slice.Slice;
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
    void init(Block inputVecBlock,
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
