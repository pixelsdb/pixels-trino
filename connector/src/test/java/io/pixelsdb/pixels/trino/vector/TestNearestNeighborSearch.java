package io.pixelsdb.pixels.trino.vector;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestNearestNeighborSearch {
    /**
     * Two files both have content
         [[0.0, 1.0E-4]]
         [[1.0, 1.0001]]
         [[2.0, 2.0001]]
         [[3.0, 3.0001]]
     * We test for a given input vector we can correctly find the k nearest vectors based on distance function, among
     * these two files
     * @throws IOException
     */
    @Test
    public void testExactNNS() throws IOException {
        double[] inputVec = new double[2];
        inputVec[0] = 2.0;
        inputVec[1] = 2.0;
        String[] listOfFiles = new String[2];
        listOfFiles[0] = System.getenv("PIXELS_S3_TEST_BUCKET_PATH") + "exactNNS-test-file3.pxl";
        listOfFiles[1] = System.getenv("PIXELS_S3_TEST_BUCKET_PATH") + "exactNNS-test-file4.pxl";
        int k = 4;
        int colId = 0;
        ExactNNS exactNNS = new ExactNNS(inputVec, listOfFiles, k, VectorDistMetrics::eucDist, colId);
        System.out.println("Nearest Neighbours:");
        double[][] expected = new double[4][2];
        expected[0][0] = 2.0;
        expected[0][1] = 2.0001;
        expected[1][0] = 2.0;
        expected[1][1] = 2.0001;
        expected[2][0] = 1.0;
        expected[2][1] = 1.0001;
        expected[3][0] = 3.0;
        expected[3][1] = 3.0001;
        List<double[]> actualRes = exactNNS.getNearestNbrs();
        for (int i=0; i<actualRes.size(); i++) {
            System.out.println(Arrays.toString(actualRes.get(i)));
            assert(Arrays.equals(expected[i], actualRes.get(i)));
        }
    }
}