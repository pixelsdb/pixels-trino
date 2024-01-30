package io.pixelsdb.pixels.trino.vector.lshnns;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.util.SparseFixedBitSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;

import static org.junit.Assert.*;

public class LSHFuncTest {

    @Test
    public void testHash() {
        int dimension = 2;
        int nBits = 4;
        LSHFunc lshFunc = new LSHFunc(2, 4, 12345L);
        System.out.println(lshFunc);
        double[] vec1 = {1.0, 0.0};
        BitSet hashedVec = lshFunc.hash(vec1);
        assert(bitsetToString(hashedVec, nBits).equals("0100"));
    }

    public static String bitsetToString(BitSet bitSet, int nBits) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0 ; i<nBits; i++) {
            if (bitSet.get(i)) {
                stringBuilder.append(1);
            } else {
                stringBuilder.append(0);
            }
        }
        return stringBuilder.toString();
    }
}