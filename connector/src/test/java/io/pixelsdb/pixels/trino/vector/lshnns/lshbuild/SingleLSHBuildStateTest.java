package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.pixelsdb.pixels.trino.vector.lshnns.LSHFuncTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;

import static org.junit.Assert.*;

public class SingleLSHBuildStateTest {
    SingleLSHBuildState state = new SingleLSHBuildState();

    @Test
    public void testInitAndUpdateBucket() {
        state.init(4, 2, 12345L);
        double[] vec = {1,2,3,4};
        double[] vec2 = {1,0,0,0};
        state.updateBuckets(vec);
        state.updateBuckets(vec2);
        System.out.println(state);
        assertEquals(2, state.buckets.size());
    }

    @Test
    public void serialize() {
        int nbits = 65;
        BitSet bitset = new BitSet(nbits);
        System.out.println(LSHFuncTest.bitsetToString(bitset,nbits));
        System.out.println(Arrays.toString(bitset.toLongArray()));
        System.out.println(bitset.length());
        long[] longArr = bitset.toLongArray();
        BitSet deserialized = BitSet.valueOf(longArr);
        System.out.println(LSHFuncTest.bitsetToString(deserialized, nbits));
    }

    @Test
    public void deserialize() {

    }
}