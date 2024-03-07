package io.pixelsdb.pixels.trino.vector.lshnns.search;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class NbrHashKeyFinder {

    BitSet original;
    int requiredDist;
    int numBits;
    //int currD = 0;
    BitSet currBitSet;
    List<BitSet> res = new ArrayList<>();

    /**
     * get
     * @param original the bitset for which we want to find the neighbours of hamming distance
     * @param requiredDist
     */
    public NbrHashKeyFinder(BitSet original, int requiredDist, int numBits) {
        this.original = original;
        this.requiredDist = requiredDist;
        this.numBits = numBits;
        currBitSet = (BitSet) original.clone();
    }

    public List<BitSet> getNeighbourHashKeys() {

        generateHammingDistanceRecursive(0, 0);

        return res;
    }

    private void generateHammingDistanceRecursive(int index, int currD) {

        if (index == numBits && currD == requiredDist) {
            res.add((BitSet) currBitSet.clone());
            return;
        }

        if (index >= numBits || currD > requiredDist) {
            return;
        }

        // Case 1: Do not flip the current bit
        generateHammingDistanceRecursive(index + 1, currD);

        // Case 2: Flip the current bit
        currBitSet.flip(index);
        generateHammingDistanceRecursive( index + 1, currD+1);
        currBitSet.flip(index); // Backtrack to the original state
    }

}
