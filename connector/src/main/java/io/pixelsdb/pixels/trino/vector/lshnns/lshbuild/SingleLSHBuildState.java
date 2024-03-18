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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.*;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class SingleLSHBuildState implements LSHBuildState {
    // stores the closest vector to the input vector among the rows that this state has seen so far.
    HashMap<BitSet, ArrayList<double[]>> buckets;
    int numBits;
    int dimension;
    LSHFunc lshFunc;
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleLSHBuildState.class).instanceSize()) +  toIntExact(ClassLayout.parseClass(PriorityQueue.class).instanceSize());
    String tableS3PathOrdered;
//    public int writeBucketThreshold = 18874368; //18mb
public int writeBucketThreshold = 1887436; //1.8mb

    public SingleLSHBuildState() {

    }

    @Override
    public void init(int dimension, int numBits) {
        this.dimension = dimension;
        this.numBits = numBits;
        this.tableS3PathOrdered = CachedLSHIndex.getInstance().getBuckets().getTableS3Path();
        // todo maybe for benchmark should set this to random?
        lshFunc = new LSHFunc(dimension, numBits, 42);
        // let the heap store the k vecs that are closest to the input vector
        // let top of the heap be the vec with largest distance that we might eliminate
        this.buckets = new HashMap<>();
    }

    /* for testing with fixed seed */
    public void init(int dimension, int numBits, long seed) {
        this.dimension = dimension;
        this.numBits = numBits;
        lshFunc = new LSHFunc(dimension, numBits, seed);
        // let the heap store the k vecs that are closest to the input vector
        // let top of the heap be the vec with largest distance that we might eliminate
        this.buckets = new HashMap<>();
    }

    @Override
    public HashMap<BitSet, ArrayList<double[]>> getBuckets() {
        return buckets;
    }

    @Override
    public void combineWithOtherState(LSHBuildState otherState) {
        HashMap<BitSet, ArrayList<double[]>> otherStateBuckets = otherState.getBuckets();
        if (otherStateBuckets == null) {
            // other state is not initialized
            return;
        }

        if (buckets==null) {
            // this state is not initialized, copy over the other state
            buckets = otherStateBuckets;
            numBits = otherState.getNumBits();
            dimension = otherState.getDimension();
            return;
        }

        otherStateBuckets.forEach(
            (otherHashKey, otherBucket)-> {
                // append to the corresponding bucket
                if (buckets.containsKey(otherHashKey)) {
                    buckets.get(otherHashKey).addAll(otherBucket);
                } else {
                    buckets.put(otherHashKey, otherBucket);
                }
            }
        );
    }

    /**
     * hash one row, i.e. one vector and add it to the corresponding bucket
     * @param vec a vector read from column data
     */
    @Override
    public void updateBuckets(double[] vec) {
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
        try {
            String pixelsFile = tableS3PathOrdered + LSHFunc.hashKeyToString(hashKey) + System.currentTimeMillis();
            Storage storage = StorageFactory.Instance().getStorage("s3");
            TypeDescription schema = TypeDescription.fromString(String.format("struct<%s:vector(%s)>", CachedLSHIndex.getInstance().getCurrColumn().getColumnName(), lshFunc.getDimension()));
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

    /**
     * again, trino has no documentation on this. But based on other implementation it seems that a blockbuilder maybe used to include values from multiple states. Can't seem to find an example of passing more than type to the factory
     * //todo probably will be eaiser to store pixels vectors as long and here use long to represent everything?
     * @param out
     */
    @Override
    public void serialize(BlockBuilder out) {
        if (buckets.isEmpty()) {
            out.appendNull();
        } else {

            BlockBuilder longBlock = out.beginBlockEntry();
            // write dimension, k
            BIGINT.writeLong(longBlock, dimension);
            BIGINT.writeLong(longBlock, numBits);

            // write each hashkey and its corresponding bucket
            BIGINT.writeLong(longBlock, buckets.size());
            buckets.forEach(
                (hashKey, bucket) -> {
                    // serialize the hash key
                    long[] hashKeyLongArr = hashKey.toLongArray();
                    BIGINT.writeLong(longBlock, hashKeyLongArr.length);
                    for (long longVal : hashKeyLongArr) {
                        BIGINT.writeLong(longBlock, longVal);
                    }
                    // serialize the bucket
                    BIGINT.writeLong(longBlock, bucket.size());
                    for (double[] vec:bucket) {
                        for (int d=0; d<vec.length; d++) {
                            BIGINT.writeLong(longBlock, Double.doubleToLongBits(vec[d]));
                        }
                    }
                }
            );
            out.closeEntry();
        }
    }

    /**
     * todo again maybe there's benefit of writing some shared stuff to the factory
     * let the factory to take the dimension,k,distFunc and input vec
     * @param block
     */
    @Override
    public void deserialize(Block block) {
        int position = 0;
        // get the dimension, k and DistFunc
        dimension = (int) BIGINT.getLong(block, position++);
        numBits = (int) BIGINT.getLong(block, position++);

        // deserialize buckets
        int numBucket = (int) BIGINT.getLong(block, position++);
        buckets = new HashMap<>(numBucket);
        // read every bucket
        for (int i=0; i<numBucket; i++) {
            // read the hashkey
            int hashKeyLen = (int) BIGINT.getLong(block, position++);
            long[] hashKeyLongArr = new long[hashKeyLen];
            for (int j=0; j<hashKeyLen; j++) {
                hashKeyLongArr[j] = BIGINT.getLong(block, position++);
            }
            BitSet hashKey = BitSet.valueOf(hashKeyLongArr);

            // read the bucket
            int bucketSize = (int) BIGINT.getLong(block, position++);
            ArrayList<double[]> bucket = new ArrayList<>(bucketSize);
            for (int j=0; j<bucketSize; j++) {
                double[] vec = new double[dimension];
                for (int d=0; d<dimension; d++) {
                    vec[d] = Double.longBitsToDouble(BIGINT.getLong(block, position++));
                }
                bucket.add(vec);
            }
            buckets.put(hashKey, bucket);
        }
    }

    //todo maybe in the future would need to be more careful with this method for optimizing performance. No document describing what should be returned
    /**
     *
     * @return the estimated size of this state. number of bytes
     */
    @Override
    public long getEstimatedSize() {
        long bucketsSize = 0;
        for (ArrayList<double[]> bucket : buckets.values()) {
            bucketsSize = bucketsSize + bucket.size() * dimension * 8L + (long)Math.ceil(numBits/8.0);
        }
        return INSTANCE_SIZE + bucketsSize;
    }

    @Override
    public int getDimension() {
        return dimension;
    }

    public int getNumBits() {
        return numBits;
    }

    @Override
    public LSHFunc getLshFunc() {
        return lshFunc;
    }

    @Override
    public String toString() {
        return "SingleLSHBuildState{" +
                "buckets=" + bucketsToString() +
                ", numBits=" + numBits +
                ", dimension=" + dimension +
                ", lshFunc=" + lshFunc +
                '}';
    }

    private String bucketsToString() {
        StringBuilder stringBuilder = new StringBuilder();
        buckets.forEach(
            (hashKey, bucket)->{
                for (int i=0; i<numBits; i++) {
                    stringBuilder.append(hashKey.get(i) ? "1" : "0");
                }
                stringBuilder.append("->[");
                for (double[] vec: bucket) {
                    stringBuilder.append(Arrays.toString(vec));
                    stringBuilder.append(",");
                }
                stringBuilder.append("],");
            });
        stringBuilder.append("}\n");
        return stringBuilder.toString();
    }

    @Override
    public void clearBuckets() {
        this.buckets = null;
    }
}
