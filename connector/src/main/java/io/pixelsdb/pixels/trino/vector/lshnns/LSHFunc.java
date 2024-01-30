package io.pixelsdb.pixels.trino.vector.lshnns;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.NormalizedRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.UncorrelatedRandomVectorGenerator;

import java.util.BitSet;

public class LSHFunc {

    RealMatrix plane_norms;
    int dimension;
    int nbits;

    public LSHFunc(int dimension, int nBits, long seed) {
        RandomGenerator randomGenerator = new JDKRandomGenerator();
        randomGenerator.setSeed(seed);

        // Create a random matrix
        plane_norms = generateRandomMatrix(nBits, dimension, randomGenerator);
        this.dimension = dimension;
        this.nbits = nBits;
    }

    // Utility method to generate a random matrix
    private static RealMatrix generateRandomMatrix(int rows, int cols, RandomGenerator randomGenerator) {
        double[][] matrixData = new double[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                // subtract 0.5 to move the interval from [0.0, 1.0] to [-0.5, 0.5]
                matrixData[i][j] = randomGenerator.nextDouble() - 0.5;
            }
        }
        return new Array2DRowRealMatrix(matrixData);
    }

    public BitSet hash(double[] vector) {
        BitSet bitSet = new BitSet(nbits);
        double[] dotProducts =  plane_norms.operate(vector);
        for (int i=0; i<dotProducts.length; i++) {
            if (dotProducts[i] > 0) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    @Override
    public String toString() {
        return "LSHFunc{" +
                "plane_norms=" + plane_norms +
                '}';
    }
}
