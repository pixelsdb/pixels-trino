package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.trino.vector.VectorAggFuncUtil;
import io.pixelsdb.pixels.trino.vector.lshnns.CachedLSHIndex;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@AggregationFunction("build_lsh")
@Description("Returns the closest vectors of the input vector")
public class BuildLSHIndexAggFunc
{
    private BuildLSHIndexAggFunc() {}

    public static final long MAX_STATE_SIZE = 204857600; // 200mb

    /**
     * update a state from a single row, i.e. a single vector
     * @param state the state that is used for aggregate many rows into one state
     */
    @InputFunction
    public static void input(@AggregationState LSHBuildState state,
                             @SqlType("array(double)") Block vecFromFile,
                             @SqlType("integer") long numPlanes)
    {
        // check input vector block
        if (numPlanes<=0) {
            return;
        }

        // initialize the state if this is an uninitialized state handling the first row
        if (state.getBuckets()==null) {
            state.init(vecFromFile.getPositionCount(), (int)numPlanes);
            String bucketsDir = String.format("s3://tiannan-test/%s_indexed/v-0-ordered/", CachedLSHIndex.getInstance().getCurrColAsStr());

            CachedLSHIndex.getInstance().updateColToBuckets(bucketsDir, state.getLshFunc());
        }

        // use the input row, i.e. a vector to update the buckets in the state
        state.updateBuckets(VectorAggFuncUtil.blockToVec(vecFromFile));
    }


    @CombineFunction
    public static void combine(@AggregationState LSHBuildState state, @AggregationState LSHBuildState otherState)
    {
        state.combineWithOtherState(otherState);
    }

    @OutputFunction(StandardTypes.JSON)
    public static void output(@AggregationState LSHBuildState state, BlockBuilder out)
    {
        if (state.getBuckets().size() == 0) {
            return;
        }
        int numVecs = state.getBuckets().size();
        int dimension = state.getDimension();
        // todo write buckets to a cached index and updating table.column->index
        // maybe have two mappings one map col->cached_index; the other col->index_file
        writeBucketsToS3Dir(CachedLSHIndex.getInstance().getBuckets().getTableS3Path(), state.getBuckets(), state.getDimension(), CachedLSHIndex.getInstance().getCurrColumn().getColumnName());

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResult = objectMapper.writeValueAsString(state.toString());
            out.writeBytes(Slices.utf8Slice(jsonResult), 0, jsonResult.length());
            out.closeEntry();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    // todo merge with LSH loader
    static void writeBucketsToS3Dir(String s3Dir, HashMap<BitSet, ArrayList<double[]>> buckets, int dimension, String colName) {

        try {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            TypeDescription schema = TypeDescription.fromString(String.format("struct<%s:vector(%s)>", colName, dimension));
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            VectorColumnVector v = (VectorColumnVector) rowBatch.cols[0];
            for (Map.Entry<BitSet, ArrayList<double[]>> entry:buckets.entrySet()) {
                BitSet hashKey = entry.getKey();
                ArrayList<double[]> bucket = entry.getValue();
                        String pixelsFile = s3Dir + LSHFunc.hashKeyToString(hashKey) + System.currentTimeMillis();
                        PixelsWriter pixelsWriter =
                                PixelsWriterImpl.newBuilder()
                                        .setSchema(schema)
                                        .setPixelStride(10000)
                                        .setRowGroupSize(64 * 1024 * 1024)
                                        .setStorage(storage)
                                        .setPath(pixelsFile)
                                        .setBlockSize(256 * 1024 * 1024)
                                        .setReplication((short) 3)
                                        .setBlockPadding(true)
                                        .setEncodingLevel(EncodingLevel.EL2)
                                        .setCompressionBlockSize(1)
                                        .build();
                        try {
                            for (double[] vec : bucket) {
                                int row = rowBatch.size++;
                                v.vector[row] = vec;
                                v.isNull[row] = false;
                                if (rowBatch.size == rowBatch.getMaxSize())
                                {
                                    pixelsWriter.addRowBatch(rowBatch);
                                    rowBatch.reset();
                                }
                            }
                            if (rowBatch.size > 0)
                            {
                                pixelsWriter.addRowBatch(rowBatch);
                                rowBatch.reset();
                            }
                            pixelsWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    };
        } catch (IOException | PixelsWriterException e) {
            e.printStackTrace();
        }
    }
}
