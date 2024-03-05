package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import org.junit.Test;

import java.util.*;

import static io.pixelsdb.pixels.trino.vector.lshnns.lshbuild.BuildLSHIndexAggFunc.writeBucketsToS3Dir;

public class BuildLSHIndexAggFuncTest {

    @Test
    public void testWriteBuckets() {
        // initialize buckets
        HashMap<BitSet, ArrayList<double[]>> buckets = new HashMap<>();

        BitSet key1 = new BitSet(4);
        key1.set(0);
        double[][] bucket1Vals = new double[][]{{1.1, 2.2}, {3.3, 4.4}};
        ArrayList<double[]> bucket1 = new ArrayList<>();
        Collections.addAll(bucket1, bucket1Vals);

        BitSet key2 = new BitSet(4);
        key2.set(3);
        double[][] bucket2Vals = new double[][]{{-1.1, -1.1}, {-3.3, -4.4}};
        ArrayList<double[]> bucket2 = new ArrayList<>();
        Collections.addAll(bucket2, bucket2Vals);

        buckets.put(key1, bucket1);
        buckets.put(key2, bucket2);

        // write buckets to s3 dir
        String s3Dir = "s3://tiannan-test/test_arr_table_indexed/v-0-ordered/";
        writeBucketsToS3Dir(s3Dir, buckets, 2, "indexed_arr_col");
    }
}