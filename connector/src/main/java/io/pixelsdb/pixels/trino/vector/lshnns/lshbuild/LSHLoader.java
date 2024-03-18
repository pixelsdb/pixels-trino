package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.trino.vector.lshnns.CachedLSHIndex;
import io.pixelsdb.pixels.trino.vector.lshnns.LSHFunc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class LSHLoader {

    String[] filesToLoad;
    LSHFunc lshFunc=null;
    int numBits;
    String schemaTableCol;
    String tableS3PathOrdered;
    Map<BitSet, ArrayList<double[]>> buckets = new HashMap<>();
    public int writeBucketThreshold = 18874368; //18mb
    public int writeId = 0;
    int totalNumRowsWrote = 0;

    public LSHLoader(String[] filesToLoad, String schemaTableCol, String tableS3Path, int numBits) {
        this.filesToLoad = filesToLoad;
        this.schemaTableCol = schemaTableCol;
        // to make the path treated as a trino table, need to put all data files into a "v-0-ordered" directory.
        this.tableS3PathOrdered = tableS3Path + "v-0-ordered/";
        this.numBits = numBits;
    }

    public void load() {
        for (String f : filesToLoad) {
            loadOneFile(f);
        }
        // write all unwritten buckets
        for (Map.Entry<BitSet, ArrayList<double[]>> entry : buckets.entrySet()) {
            BitSet hashKey = entry.getKey();
            ArrayList<double[]> bucket = entry.getValue();
            if (bucket.size() > 0) {
                writeOneBucketToS3(hashKey, bucket);
            }
        }
    }

    private double[] lineToVec(String line) {
        String[] numStrs = line.split("\\s+");
        double[] vec = new double[numStrs.length];
        for (int i = 0; i < numStrs.length; i++) {
            vec[i] = Double.parseDouble(numStrs[i]);
        }
        return vec;
    }

    private void loadOneFile(String file) {
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            // use the first vector to finish initialization of the loader
            double[] firstVec = lineToVec(br.readLine());
            if (lshFunc == null) {
                this.lshFunc = new LSHFunc(firstVec.length, numBits, 42);
                CachedLSHIndex.getInstance().updateColToBuckets(schemaTableCol, tableS3PathOrdered, lshFunc);
            }
            updateBuckets(firstVec);

            // process the rest of the vecs
            while ((line = br.readLine()) != null) {
                updateBuckets(lineToVec(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * insert a vector to the correct bucket. If the bucket is bigger than the threshold, we write the bucket to s3 and clear the bucket.
     * @param vec
     */
    private void updateBuckets(double[] vec) {
        BitSet hashVal = lshFunc.hash(vec);
        if (buckets.containsKey(hashVal)) {
            ArrayList<double[]> bucket = buckets.get(hashVal);
            bucket.add(vec);
            if (bucket.size() * lshFunc.getDimension() * 8 > writeBucketThreshold) {
                writeOneBucketToS3(hashVal, bucket);
                bucket.clear();
            }
        } else {
            ArrayList<double[]> arrayList = new ArrayList<>();
            arrayList.add(vec);
            buckets.put(hashVal, arrayList);
        }
    }

    private void writeOneBucketToS3(BitSet hashKey, ArrayList<double[]> bucket) {
        totalNumRowsWrote += bucket.size();
        try {
            String pixelsFile = tableS3PathOrdered + LSHFunc.hashKeyToString(hashKey) + System.currentTimeMillis();
            writeId++;
            Storage storage = StorageFactory.Instance().getStorage("s3");
            TypeDescription schema = TypeDescription.fromString(String.format("struct<%s:vector(%s)>", schemaTableCol.split("\\.")[2], lshFunc.getDimension()));
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
            try {
                for (double[] vec : bucket) {
                    int row = rowBatch.size++;
                    v.vector[row] = vec;
                    v.isNull[row] = false;
                    if (rowBatch.size == rowBatch.getMaxSize()) {
                        pixelsWriter.addRowBatch(rowBatch);
                        rowBatch.reset();
                    }
                }
                if (rowBatch.size > 0) {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
                pixelsWriter.close();
            } catch (IOException | PixelsWriterException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
