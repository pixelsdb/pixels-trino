package io.pixelsdb.pixels.trino.vector.lshnns.search;

import io.pixelsdb.pixels.trino.vector.VectorDistFunc;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.pixelsdb.pixels.trino.vector.exactnns.SingleExactNNSState;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import org.junit.Test;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static org.junit.Assert.*;

public class LSHSearchUDFTest {

    @Test
    public void testGeneratingNeighbouringBitSets() {
        int numBits = 4;
        BitSet bitSet = new BitSet(numBits);
        bitSet.set(1);
        NbrBitSetsFinder nbrBitSetsFinder = new NbrBitSetsFinder(bitSet, 1, numBits);
        List<BitSet> nbrBitSets = nbrBitSetsFinder.getNeighbourBitSets();
        System.out.println(nbrBitSets);
    }

    @Test
    public void testGeneratingNeighbouringBitSetsDist0() {
        int numBits = 4;
        BitSet bitSet = new BitSet(numBits);
        bitSet.set(1);
        NbrBitSetsFinder nbrBitSetsFinder = new NbrBitSetsFinder(bitSet, 0, numBits);
        List<BitSet> nbrBitSets = nbrBitSetsFinder.getNeighbourBitSets();
        System.out.println(nbrBitSets);
    }

    @Test
    public void testBfsIndexFiles() {
        double[] inputVec = {0.0, 0.0};
        LSHFunc lshFunc = new LSHFunc(2, 4, 42);
        String bucketsDir = "s3://tiannan-test/test_arr_table_indexed/v-0-ordered";
        int k=1;
        BitSet inputVecHash = lshFunc.hash(inputVec);
        Comparator<double[]> comparator = new SingleExactNNSState.VecDistComparator(inputVec, VectorDistFuncs::eucDist);
        PriorityQueue<double[]> nearestVecs = new PriorityQueue<>(2, comparator);
        LSHSearchUDF.bfsIndexFiles(nearestVecs, inputVecHash, k, lshFunc, bucketsDir);
    }
}