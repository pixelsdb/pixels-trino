package io.pixelsdb.pixels.trino.vector.lshnns.search;

import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.pixelsdb.pixels.trino.vector.exactnns.SingleExactNNSState;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import org.junit.Test;

import java.util.*;

public class LSHSearchUDFTest {

    @Test
    public void testGeneratingNeighbouringBitSets() {
        int numBits = 4;
        BitSet bitSet = new BitSet(numBits);
        bitSet.set(1);
        NbrHashKeyFinder nbrHashKeyFinder = new NbrHashKeyFinder(bitSet, 1, numBits);
        List<BitSet> nbrBitSets = nbrHashKeyFinder.getNeighbourHashKeys();
        System.out.println(nbrBitSets);
    }

    @Test
    public void testGeneratingNeighbouringBitSetsDist0() {
        int numBits = 4;
        BitSet bitSet = new BitSet(numBits);
        bitSet.set(1);
        NbrHashKeyFinder nbrHashKeyFinder = new NbrHashKeyFinder(bitSet, 0, numBits);
        List<BitSet> nbrBitSets = nbrHashKeyFinder.getNeighbourHashKeys();
        System.out.println(nbrBitSets);
    }

    @Test
    public void testBfsIndexFiles() {
        double[] inputVec = {0, 0};
        LSHFunc lshFunc = new LSHFunc(2, 4, 42);
        String bucketsDir = "s3://tiannan-test/test_arr_table_indexed/v-0-ordered/";
        int k=4;
        BitSet inputVecHash = lshFunc.hash(inputVec);
        Comparator<double[]> comparator = new SingleExactNNSState.VecDistComparator(inputVec, VectorDistFuncs::eucDist);
        PriorityQueue<double[]> nearestVecs = new PriorityQueue<>(k, comparator.reversed());


        LSHSearchUDF.bfsIndexFiles(nearestVecs, inputVecHash, k, lshFunc, bucketsDir);
        int numVecs = nearestVecs.size();
        double[][] nearestVecsArr = new double[numVecs][];
        for (int i=0; i<numVecs; i++) {
            nearestVecsArr[i] = nearestVecs.poll();
        }
        // sort the nearest vecs from smaller dist to larger dist
        Arrays.sort(nearestVecsArr, nearestVecs.comparator().reversed());
        for (double[] vec:nearestVecsArr) {
            System.out.println(Arrays.toString(vec));
        }
    }
}