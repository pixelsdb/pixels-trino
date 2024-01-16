package io.pixelsdb.pixels.trino.vector.exactnns;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.trino.vector.VectorDistFunc;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This class implements a multi-threaded algorithm that utilizes priority queue to obtain nearest neighbours,
 * i.e. vectors that are closest to the input vector according to the chosen distance metric.
 */
//public class ExactNNS {
//
//    static final int NUM_CORE = Runtime.getRuntime().availableProcessors();
//    String[] allFilesToScan;
//    int k;
//    VecComparator vecDistDescComparator;
//    int colId;
//
//    /**
//     * @param inputVec the input vector for whom we want to find the nearest neighbors
//     * @param listOfFiles list of datafiles, usually should be all the files belong to a column
//     * @param k the maximum number of nearest neighbors to be returned
//     * @param vectorDistFunc vector distance metric, e.g. euclidean distance
//     * @param colId the column id of the vector column that we want to find nearest neighbor in
//     */
//    public ExactNNS(double[] inputVec, String[] listOfFiles, int k, VectorDistFunc vectorDistFunc, int colId)
//    {
//        this.allFilesToScan = listOfFiles;
//        this.k = k;
//        this.vecDistDescComparator = new VecComparator(inputVec, vectorDistFunc);
//        this.colId = colId;
//    }
//
//    /**
//     * For a vector column, compute a list of vectors that are closest to the inputVec, using vectorDistFunc as the
//     * distance metric. This method uses multi-threading. Each thread is responsible a subset of data files. Each thread
//     * returns a priority queue containing the k nearest neighbours found by that thread. Then after all threads are
//     * finished, we combine all the priority queues returned by all threads to get the final k nearest neighbours.
//     * @return
//     * @throws IOException
//     */
//    public List<double[]> getNearestNbrs() throws IOException
//    {
//        // todo for each set of files we can have a thread with a heap searching for the nearest vector
//        // todo and in the end we merge all the results
//        // todo maybe can also make use of multiple machines? Probably should prioritize implementing different indexing
//        // methods
//        ExecutorService executorService = Executors.newFixedThreadPool(NUM_CORE);
//        List<Future<PriorityQueue<double[]>>> futurePQs = new ArrayList<>();
//        int numFilesPerThread = (int) Math.ceil(1.0 * allFilesToScan.length / NUM_CORE);
//        for (int i=0; i<NUM_CORE; i++) {
//            int startIndex = i * numFilesPerThread;
//            int endIndex = Math.min((i+1) * numFilesPerThread, allFilesToScan.length);
//            futurePQs.add(executorService.submit(()->exactNNSOneThread(Arrays.copyOfRange(allFilesToScan, startIndex, endIndex))));
//            if (endIndex == allFilesToScan.length) {
//                // all files have been assigned to a thread
//                break;
//            }
//        }
//
//        // merge each thread's max heap.
//        int totalElements = 0;
//        for (Future<PriorityQueue<double[]>> futurePQ : futurePQs) {
//            try {
//                totalElements += futurePQ.get().size();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        }
//        double[][] PQCombineArray = new double[totalElements][];
//        int i = 0;
//        for (Future<PriorityQueue<double[]>> futurePQ : futurePQs) {
//            try {
//                for (double[] vec:futurePQ.get()) {
//                    PQCombineArray[i++] = vec;
//                }
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        }
//        Arrays.sort(PQCombineArray, vecDistDescComparator);
//        int resLen = Math.min(PQCombineArray.length, k);
//        List<double[]> res = new ArrayList<>(resLen);
//        for (int j = 0; j < resLen; j++) {
//            res.add(PQCombineArray[j]);
//        }
//        return res;
//    }
//
//    /**
//     *
//     * @param files data files to scan for this thread
//     * @return a priority queue that stores the top k vectors in files that are closest to inputVec
//     * @throws IOException
//     */
//    private PriorityQueue<double[]> exactNNSOneThread(String[] files) throws IOException
//    {
//        PriorityQueue<double[]> nearestVecs = new PriorityQueue<>(k, vecDistDescComparator.reversed());
//        Storage storage = StorageFactory.Instance().getStorage("s3");
//        for (String file:files) {
//            PixelsReader reader = PixelsReaderImpl.newBuilder()
//                    .setStorage(storage)
//                    .setPath(file)
//                    .setPixelsFooterCache(new PixelsFooterCache())
//                    .build();
//
//            TypeDescription schema = reader.getFileSchema();
//            System.out.println(schema);
//            List<String> fieldNames = schema.getFieldNames();
//            System.out.println("fieldNames: " + fieldNames);
//            String[] cols = new String[fieldNames.size()];
//            for (int i = 0; i < fieldNames.size(); i++) {
//                cols[i] = fieldNames.get(i);
//            }
//
//            PixelsReaderOption option = new PixelsReaderOption();
//            option.skipCorruptRecords(true);
//            option.tolerantSchemaEvolution(true);
//            option.includeCols(cols);
//            PixelsRecordReader recordReader = reader.read(option);
//            System.out.println("recordReader.getCompletedRows():" + recordReader.getCompletedRows());
//            System.out.println("reader.getRowGroupInfo(0).getNumberOfRows():" + reader.getRowGroupInfo(0).getNumberOfRows());
//            int batchSize = 10000;
//            VectorizedRowBatch rowBatch;
//
//            int numRows = 0;
//            int numBatches = 0;
//            // loop through all the rowbatch
//            while (true) {
//                rowBatch = recordReader.readBatch(batchSize);
//                System.out.println("rowBatch: \n" + rowBatch);
//                numBatches++;
//
//                // for the target column in each rowbatch, loop through each row and keep updating the priority queue
//                VectorColumnVector vectorColumnVector = (VectorColumnVector) rowBatch.cols[colId];
//                for (int i=0; i<rowBatch.size; i++) {
//                    double[] vec = vectorColumnVector.vector[i];
//                    if (nearestVecs.size() < k) {
//                        nearestVecs.add(vec);
//                    } else if (vecDistDescComparator.compare(nearestVecs.peek(), vec) > 0) {
//                        // if the vec with largest dist in PQ has distance larger than vec, then we remove the top
//                        // of PQ and insert vec
//                        nearestVecs.poll();
//                        nearestVecs.add(vec);
//                    }
//                }
//
//                if (rowBatch.endOfFile) {
//                    numRows += rowBatch.size;
//                    break;
//                }
//                numRows += rowBatch.size;
//            }
//            System.out.println("numBatches:" + numBatches + ", numRows:" + numRows);
//            reader.close();
//        }
//        return nearestVecs;
//    }
//
//}
