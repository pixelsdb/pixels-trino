package io.pixelsdb.pixels.trino.vector.experiements;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class PrepareExperimentData {
    public static void main(String[] args) {
        //String filename = "/Users/sha/Desktop/EPFL/master-thesis/wiki-news-300d-154.vec";
        //String filename = "/Users/sha/Desktop/EPFL/master-thesis/wiki-news-300d-1M.vec";
        String filename = "/home/ubuntu/vecdb-experiments/wiki-news-300d-1M.vec";
        String s3Dir = "s3://tiannan-test/experiments/embd_1m_new/v-0-ordered/";
        writeFastTextFileToPxlFiles(filename, s3Dir);
    }

    /**
     * write a Fast text file to .pxl files on s3.
     * @param fastTextFile
     * @param s3Dir s3 directory to write .pxl files to
     */
    public static void writeFastTextFileToPxlFiles(String fastTextFile, String s3Dir) {

        // read words and embeddingBatch
        String line;
        int numLines = 1;
        int dimension = 0;
        int lineId = 0;
        int batchId = 0;
        final int BATCH_SIZE = 10240;
        HashSet<double[]> vecSet = new HashSet<>();
        List<double[]> embeddingBatch = new ArrayList<>(BATCH_SIZE);
        try (BufferedReader br = new BufferedReader(new FileReader(fastTextFile))) {
            // Process the first line which contains total number of entries in the file and the dimension of embeddingBatch
            line = br.readLine();
            String[] nums = line.split("\\s+");
            numLines = Integer.parseInt(nums[0]);
            dimension = Integer.parseInt(nums[1]);
            while ((line = br.readLine()) != null && lineId < numLines) {
                String[] wordAndEmbedding = line.split("\\s+");
//                String word = wordAndEmbedding[0];
//                if (words.contains(word)) {
//                    System.out.println( "Strange!!! " + word + "already exists" );
//                } else {
//                    words.add(word);
//                }
                double[] embd = new double[dimension];
                for (int i=0; i<dimension; i++) {
                    embd[i] = Double.parseDouble(wordAndEmbedding[i+1]);
                }
                embeddingBatch.add(embd);
                if (vecSet.contains(embd)) {
                    System.out.println("!!!same vec insert twice");
                } else {
                    vecSet.add(embd);
                }
                if (embeddingBatch.size() == BATCH_SIZE) {
                    writeEmbeddingBatchToOneFile(embeddingBatch, s3Dir + batchId + ".pxl");
                    System.out.println("batch" + batchId + "has been writen to s3");
                    embeddingBatch = new ArrayList<>(BATCH_SIZE);
                    batchId++;
                }
                lineId++;
            }
            // write last batch if there are some left
            if (embeddingBatch.size() > 0) {
                writeEmbeddingBatchToOneFile(embeddingBatch, s3Dir + batchId + ".pxl");
                batchId++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeEmbeddingBatchToOneFile(List<double[]> embeddings, String file) throws IOException {
        Storage storage = StorageFactory.Instance().getStorage("s3");
        int dimension = embeddings.get(0).length;
        String schemaStr = String.format("struct<embd_col:vector(%s)>", dimension);
        try
        {
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            VectorColumnVector vecVec = (VectorColumnVector) rowBatch.cols[0];

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setStorage(storage)
                            .setPath(file)
                            .setBlockSize(256 * 1024 * 1024)
                            .setReplication((short) 3)
                            .setBlockPadding(true)
                            .setEncodingLevel(EncodingLevel.EL2)
                            .setCompressionBlockSize(1)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);

            int numRows = embeddings.size();
            // write all the rows batch by batch
            for (int i = 0; i < numRows; i++)
            {
                int rowId = rowBatch.size++;
                // add ith embedding to be the rowId-th vector in the batch
                vecVec.vector[rowId] = new double[dimension];
                for (int d=0; d<dimension; d++) {
                    vecVec.vector[rowId][d] = embeddings.get(i)[d];
                }
                vecVec.isNull[rowId] = false;

                // add row batch if it is full
                if (rowBatch.size == rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + file);
                    rowBatch.reset();
                }
            }

            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + file);
                rowBatch.reset();
            }

            pixelsWriter.close();
        } catch (IOException | PixelsWriterException e)
        {
            e.printStackTrace();
        }
    }

//    public static void writeEmbeddings(String[] words, double[][] embeddings) throws IOException {
//        String pixelsFile = "s3://tiannan-test/experiments/embd_1m/v-0-ordered/1.pxl";
//        Storage storage = StorageFactory.Instance().getStorage("s3");
//        int dimension = embeddings[0].length;
//        String schemaStr = String.format("struct<embd_col:vector(%s), word_col:varchar(50)>", dimension);
//        try
//        {
//            TypeDescription schema = TypeDescription.fromString(schemaStr);
//            VectorizedRowBatch rowBatch = schema.createRowBatch();
//            VectorColumnVector vecVec = (VectorColumnVector) rowBatch.cols[0];
//            BinaryColumnVector strVec = (BinaryColumnVector) rowBatch.cols[1];
//
//            PixelsWriter pixelsWriter =
//                    PixelsWriterImpl.newBuilder()
//                            .setSchema(schema)
//                            .setPixelStride(10000)
//                            .setRowGroupSize(64 * 1024 * 1024)
//                            .setStorage(storage)
//                            .setPath(pixelsFile)
//                            .setBlockSize(256 * 1024 * 1024)
//                            .setReplication((short) 3)
//                            .setBlockPadding(true)
//                            .setEncodingLevel(EncodingLevel.EL2)
//                            .setCompressionBlockSize(1)
//                            .build();
//
//            long curT = System.currentTimeMillis();
//            Timestamp timestamp = new Timestamp(curT);
//
//            int numRows = words.length;
//            // write all the rows batch by batch
//            for (int i = 0; i < numRows; i++)
//            {
//                int rowId = rowBatch.size++;
//                // add embedding
//                vecVec.vector[rowId] = new double[dimension];
//                for (int d=0; d<dimension; d++) {
//                    vecVec.vector[rowId][d] = embeddings[rowId][d];
//                }
//                vecVec.isNull[rowId] = false;
//
//                // add word
//                strVec.add(words[rowId].getBytes());
//                strVec.isNull[rowId] = false;
//
//                // write rowbatch
//                if (rowBatch.size == rowBatch.getMaxSize())
//                {
//                    pixelsWriter.addRowBatch(rowBatch);
//                    System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
//                    rowBatch.reset();
//                }
//
//            }
//
//            if (rowBatch.size != 0)
//            {
//                pixelsWriter.addRowBatch(rowBatch);
//                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
//                rowBatch.reset();
//            }
//
//            pixelsWriter.close();
//        } catch (IOException | PixelsWriterException e)
//        {
//            e.printStackTrace();
//        }
//    }
}
