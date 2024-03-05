package io.pixelsdb.pixels.trino.vector.lshnns.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.trino.vector.lshnns.CachedLSHIndex;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import io.pixelsdb.pixels.trino.vector.lshnns.lshbuild.LSHBuildState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import java.util.BitSet;

@AggregationFunction("lsh_search_multinode")
@Description("Returns the aprroximately closest vectors of the input vector")
public class LSHSearchAggFunc {

    private LSHSearchAggFunc() {}

    /**
     * update a state from a single row, i.e. a single vector
     * @param state the state that is used for aggregate many rows into one state
     */
    @InputFunction
    public static void input(@AggregationState LSHBuildState state,
                             @SqlType("array(double)") Block vecFromFile,
                             @SqlType("array(double)") Block inputVec,
                             @SqlType(StandardTypes.VARCHAR) Slice sliceForVectorDistFunc,
                             @SqlType("integer") long k)
    {
        // todo somehow pass information to pixels reader? Or change pixels reader to check
        // the cached index?
        String bucketsDir = CachedLSHIndex.getInstance().getBuckets().getTableS3Path();
        if (bucketsDir==null) {
            return;
        }
        LSHFunc lshFunc = CachedLSHIndex.getInstance().getBuckets().getLshFunc();
        assert(lshFunc != null); // if bucketsDir exists, then so should LshFunc
        BitSet inputVecHash = null; // todo hash input func here
    }


    @CombineFunction
    public static void combine(@AggregationState LSHBuildState state, @AggregationState LSHBuildState otherState)
    {
        state.combineWithOtherState(otherState);
    }

    @OutputFunction(StandardTypes.JSON)
    public static void output(@AggregationState LSHBuildState state, BlockBuilder out)
    {

        int numVecs = state.getBuckets().size();
        int dimension = state.getDimension();
        // todo write buckets to a cached index and updating table.column->index
        // maybe have two mappings one map col->cached_index; the other col->index_file
        state.getBuckets();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResult = objectMapper.writeValueAsString(state.toString());
            out.writeBytes(Slices.utf8Slice(jsonResult), 0, jsonResult.length());
            out.closeEntry();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
