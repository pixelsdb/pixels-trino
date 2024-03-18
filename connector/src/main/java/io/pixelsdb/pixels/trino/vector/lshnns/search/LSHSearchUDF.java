package io.pixelsdb.pixels.trino.vector.lshnns.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.pixelsdb.pixels.trino.vector.VectorAggFuncUtil;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.pixelsdb.pixels.trino.vector.exactnns.SingleExactNNSState;
import io.pixelsdb.pixels.trino.vector.lshnns.CachedLSHIndex;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class LSHSearchUDF {
    private static final Logger logger = Logger.get(LSHSearchUDF.class);

    private LSHSearchUDF() {}

    @ScalarFunction("lsh_search")
    @Description("calculate the Euclidean distance between two vectors")
    @SqlType(StandardTypes.JSON)
    @SqlNullable
    public static Slice lshSearch(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice inputVecSlice,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice distFuncSlice,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
            @SqlType("integer") long k)
    {
        VectorDistFuncs.DistFuncEnum vectorDistFuncEnum = VectorAggFuncUtil.sliceToDistFunc(distFuncSlice);
        PixelsColumnHandle pixelsColumnHandle = VectorAggFuncUtil.sliceToColumn(column);
        CachedLSHIndex cachedLSHIndex = CachedLSHIndex.getInstance();
        cachedLSHIndex.setCurrColumn(pixelsColumnHandle);
        CachedLSHIndex.Buckets buckets = CachedLSHIndex.getInstance().getBuckets();
        if (buckets==null) {
            throw new IllegalArgumentException("column" + pixelsColumnHandle + "doesn't exist in LSH index");
        }
        LSHFunc lshFunc = buckets.getLshFunc();
        assert(lshFunc != null); // if bucketsDir exists, then so should LshFunc
        double[] inputVec = VectorAggFuncUtil.sliceToVec(inputVecSlice);
        BitSet inputVecHash = lshFunc.hash(inputVec);

        Comparator<double[]> comparator = new SingleExactNNSState.VecDistComparator(inputVec, vectorDistFuncEnum.getDistFunc());
        PriorityQueue<double[]> nearestVecs = new PriorityQueue<>((int)k, comparator.reversed());

        bfsIndexFiles(nearestVecs, inputVecHash, k, lshFunc, buckets.getTableS3Path());

        // do a final sort and then output as json string
        int numVecs = nearestVecs.size();
        double[][] nearestVecsArr = new double[numVecs][];
        for (int i=0; i<numVecs; i++) {
            nearestVecsArr[i] = nearestVecs.poll();
        }
        // sort the nearest vecs from smaller dist to larger dist
        Arrays.sort(nearestVecsArr, nearestVecs.comparator().reversed());
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return Slices.utf8Slice(objectMapper.writeValueAsString(nearestVecsArr));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * do a BFS based on the input vector's hash value. The S3 dir consists of files representing each bucket. We search
     * from the file name that matches the input vector's hash. If this file doesn't exist or provide less than k files, we
     * check files storing vecs whose hash that have hamming distance 1 from the input vec's hash
     * @param nearestVecs
     * @param inputVecHash
     * @param k
     * @param lshFunc
     * @param bucketsDir
     */
    public static void bfsIndexFiles(PriorityQueue<double[]> nearestVecs, BitSet inputVecHash, long k, LSHFunc lshFunc, String bucketsDir) {
        // bfs around the inputVec's hash until we found k nearestVecs
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        int distToInputVecHash = 0;

        // get all files in the dir
//        Map<String, List<String>> hashKeyStrToFiles = new HashMap<>();
//        List<String>  allFiles = S3FilesUtil.listBucketObjects(bucketsDir);
//        for (String f : allFiles) {
//            String hashKeyStr = S3FilesUtil.fileNameToHashKeyStr(f);
//            if (hashKeyStrToFiles.containsKey(hashKeyStr)) {
//                hashKeyStrToFiles.get(hashKeyStr).add(f);
//            } else {
//                ArrayList<String> files = new ArrayList<>();
//                files.add(f);
//                hashKeyStrToFiles.put(hashKeyStr, files);
//            }
//        }

        // stop bfs if we found k closest vecs, or we searched through all possible files
        while (nearestVecs.size() < k && distToInputVecHash <= lshFunc.getNumBits()) {
            NbrHashKeyFinder nbrHashKeyFinder = new NbrHashKeyFinder(inputVecHash, distToInputVecHash, lshFunc.getNumBits());
            List<BitSet> nbrHashKeys = nbrHashKeyFinder.getNeighbourHashKeys();
            List<Future> futures = new ArrayList<>(nbrHashKeys.size());
            // each thread reads one file and update the pq
            for (BitSet nbrBitSet : nbrHashKeys) {
                // get the files we need to read. For some hash keys, we might don't have any file with that key.
                List<String> filesToRead = S3FilesUtil.listFilesWithHashKey(bucketsDir, LSHFunc.hashKeyToString(nbrBitSet));
                if (filesToRead != null) {
                    for (String file : filesToRead) {
                        // todo need to add the s3 path to file. The file here is just file name.
                        futures.add(executorService.submit(() -> readOneFileAndUpdatePQ(bucketsDir + file, nearestVecs, (int) k)));
                    }
                }

            }
            // wait till all threads are finished, then we increase the dist and search more files if less than k vecs are found
            for (Future f:futures) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            distToInputVecHash++;
        }
        // All threads should have finished now. We shut down immediately.
        executorService.shutdownNow();
    }

    /**
     *
     * @param file the s3path of the s3 file we want to read
     * @return num of rows successfully read
     */
    private static void readOneFileAndUpdatePQ(String file, PriorityQueue<double[]> nearestVecs, int k) {
        try {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(file)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();

            TypeDescription schema = reader.getFileSchema();
            System.out.println(schema);
            List<String> fieldNames = schema.getFieldNames();
            System.out.println("fieldNames: " + fieldNames);
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            int batchSize = 10000;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int numRows = 0;
            int numBatches = 0;
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                updateNearestVecs(nearestVecs, rowBatch, k);
                numBatches++;
                String result = rowBatch.toString();
                len += result.length();
                if (rowBatch.endOfFile) {
                    numRows += rowBatch.size;
                    break;
                }
                numRows += rowBatch.size;
            }
            reader.close();

        } catch (IOException e) {
            logger.debug("file " + file + "doesn't exist");
        }
    }

    private synchronized static void updateNearestVecs(PriorityQueue<double[]> nearestVecs, VectorizedRowBatch rowBatch, int k) {
        VectorColumnVector vcv = (VectorColumnVector) rowBatch.cols[0];
        for (int i=0; i<rowBatch.size; i++) {
            double[] vec = vcv.vector[i];
            if (nearestVecs.size() < k) {
                nearestVecs.add(vec);
                // reverse the comparator back to normal comparator. Recall that we use reversed comparator for pq to use it as max heap
            } else if (nearestVecs.comparator().reversed().compare(nearestVecs.peek(), vec) > 0) {
                // if the vec with largest dist in PQ has distance larger than vec, then we remove the top
                // of PQ and insert vec
                nearestVecs.poll();
                nearestVecs.add(vec);
            }
        }
    }

}
