package io.pixelsdb.pixels.trino.vector;

import io.airlift.slice.Slices;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class VectorAggFuncUtilTest {
    @Test
    public void testSliceToVec() {
        String jsonString = "[1.0, 2.0, 3.0, 4.0, 5.0]";
        double[] vec = VectorAggFuncUtil.sliceToVec(Slices.utf8Slice(jsonString));
        System.out.println(Arrays.toString(vec));
    }
}