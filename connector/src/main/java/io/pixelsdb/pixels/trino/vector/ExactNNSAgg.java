package io.pixelsdb.pixels.trino.vector;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.util.PriorityQueue;

@AggregationFunction("array_sum")
public class ExactNNSAgg
{
    private ExactNNSAgg() {}


    /**
     * initialize a state from a single row, i.e. a single vector
     * @param state
     * @param block a block representing an array of double
     */
    @InputFunction
    public static void input(@AggregationState ExactNNSState state, @SqlType("array(double)") Block  block, @SqlType(StandardTypes.VARCHAR) Slice strForVectorDistFunc, @SqlType("integer") long k)
    {

    }


    @CombineFunction
    public static void combine(@AggregationState ExactNNSState state, @AggregationState ExactNNSState otherState)
    {
        // todo update the PriorityQueue that stores nearest vectors in state, using the PriorityQueue in otherState
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState ExactNNSState state, BlockBuilder out)
    {
// todo convert the priority queue in state to json string
    }
}
