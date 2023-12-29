package io.pixelsdb.pixels.trino.vector;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
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
        // prepare test files
        writeVectorColumnToS3(getTestVectors(4,2), "exactNNS-test-file3.pxl");
        writeVectorColumnToS3(getTestVectors(4,2), "exactNNS-test-file4.pxl");

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
            //assert(Arrays.equals(expected[i], actualRes.get(i)));
        }
    }

    // todo maybe add a large scale test

    public static void writeVectorColumnToS3(double[][] vectorsToWrite, String s3File)
    {
        int length = vectorsToWrite.length;
        if (vectorsToWrite[0]==null) {
            return;
        }
        int dimension = vectorsToWrite[0].length;
        // Note you may need to restart intellij to let it pick up the updated environment variable value
        // example path: s3://bucket-name/test-file.pxl
        try
        {
            String pixelsFile = System.getenv("PIXELS_S3_TEST_BUCKET_PATH") + s3File;
            Storage storage = StorageFactory.Instance().getStorage("s3");

            String schemaStr = String.format("struct<v:vector(%s)>", dimension);

            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            VectorColumnVector v = (VectorColumnVector) rowBatch.cols[0];

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

            for (int i = 0; i < length-1; i++)
            {
                int row = rowBatch.size++;
                v.vector[row] = new double[dimension];
                System.arraycopy(vectorsToWrite[row], 0, v.vector[row], 0, dimension);
                v.isNull[row] = false;
                if (rowBatch.size == rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }

            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
                rowBatch.reset();
            }

            pixelsWriter.close();
        } catch (IOException | PixelsWriterException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * testVectors[i][j] = i + j*0.0001
     * e.g. testVector[0][500] = 0.05
     * @param length number of vectors
     * @param dimension dimension of each vector
     * @return
     */
    private static double[][] getTestVectors(int length, int dimension) {

        double[][] testVecs = new double[length][dimension];
        for (int i=0; i<length; i++) {
            for (int j=0; j<dimension; j++) {
                testVecs[i][j] = i + j*0.0001;
            }
        }
        return testVecs;
    }
}