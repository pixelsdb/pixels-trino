package io.pixelsdb.pixels.trino.vector.exactnns;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.trino.vector.VectorAggFuncUtil;
import io.pixelsdb.pixels.trino.vector.VectorDistFunc;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.PriorityQueue;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

//todo maybe can use typed heap instead?
// for serializing state to block, maybe no need to send k, distfunc and
// input vec becuase serialization is for sending to one state to another
// machine to combine with other states. The other states should already have
// the same

// todo maybe can even use min_by(x,y,n)
// createArrayBigintBlock shows another way to create block  for an array

//KISS

public class SingleExactNNSState implements ExactNNSState {
    // stores the closest vector to the input vector among the rows that this state has seen so far.
    PriorityQueue<double[]> nearestVecs;
    double[] inputVec;
    int k;
    int dimension;
    VecDistComparator vecDistDescComparator;
    VectorDistFuncs.DistFuncEnum vectorDistFuncEnum;
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleExactNNSState.class).instanceSize()) +  toIntExact(ClassLayout.parseClass(PriorityQueue.class).instanceSize());

    public SingleExactNNSState() {

    }

    @Override
    public void init(double[] inputVec,
                     int dimension,
                     Slice distFuncSlice,
                     int k) {
        this.inputVec = inputVec;
        this.dimension = dimension;
        this.k = k;
        this.vectorDistFuncEnum = VectorAggFuncUtil.sliceToDistFunc(distFuncSlice);
        vecDistDescComparator = new VecDistComparator(inputVec, vectorDistFuncEnum.getDistFunc());
        // let the heap store the k vecs that are closest to the input vector
        // let top of the heap be the vec with largest distance that we might eliminate
        this.nearestVecs = new PriorityQueue<>(k, vecDistDescComparator.reversed());
    }


    @Override
    public PriorityQueue<double[]> getNearestVecs() {
        return nearestVecs;
    }

    @Override
    public void combineWithOtherState(ExactNNSState otherState) {
        PriorityQueue<double[]> otherNearestVecs = otherState.getNearestVecs();
        if (otherNearestVecs == null) {
            // other state is not initialized
            return;
        }

        if (nearestVecs==null) {
            // this state is not initialized, copy over the other state
            nearestVecs = otherNearestVecs;
            inputVec = otherState.getInputVec();
            k = otherState.getK();
            dimension = otherState.getDimension();
            vecDistDescComparator = otherState.getVecDistDescComparator();
            vectorDistFuncEnum = otherState.getVectorDistFuncEnum();
            return;
        }

        for (double[] vec : otherNearestVecs) {
            updateNearestVecs(vec);
        }
    }

    /**
     * use one row, i.e. one vector to potentially update the priority queue storing the k nearest neighbours this state has seen so far.
     * @param vec a vector read from column data
     */
    @Override
    public void updateNearestVecs(double[] vec) {
        if (nearestVecs.size() < k) {
            nearestVecs.add(vec);
        } else if (vecDistDescComparator.compare(nearestVecs.peek(), vec) > 0) {
            // if the vec with largest dist in PQ has distance larger than vec, then we remove the top
            // of PQ and insert vec
            nearestVecs.poll();
            nearestVecs.add(vec);
        }
    }

    /**
     * again, trino has no documentation on this. But based on other implementation it seems that a blockbuilder maybe used to include values from multiple states. Can't seem to find an example of passing more than type to the factory
     * //todo probably will be eaiser to store pixels vectors as long and here use long to represent everything?
     * @param out
     */
    @Override
    public void serialize(BlockBuilder out) {
        if (nearestVecs.isEmpty()) {
            out.appendNull();
        } else {

            BlockBuilder longBlock = out.beginBlockEntry();
            // write dimension, k, distFunc
            BIGINT.writeLong(longBlock, dimension);
            BIGINT.writeLong(longBlock, k);
            BIGINT.writeLong(longBlock, vectorDistFuncEnum.ordinal());

            // write the inputVec
            for (double element:inputVec) {
                BIGINT.writeLong(longBlock, Double.doubleToLongBits(element));//todo convert this to long representation);
            }

            // write nearestVecs
            BIGINT.writeLong(longBlock, nearestVecs.size());
            for (double[] vec:nearestVecs) {
                for (double element:vec) {
                    BIGINT.writeLong(longBlock, Double.doubleToLongBits(element));
                }
            }
            longBlock.closeEntry();
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
        k = (int) BIGINT.getLong(block, position++);
        vectorDistFuncEnum = VectorDistFuncs.DistFuncEnum.getDistFuncEnumByOrdinal((int)BIGINT.getLong(block, position++));

        // deserialize the input vector
        this.inputVec = new double[dimension];
        for (int d=0; d<dimension; d++) {
            inputVec[d] = Double.longBitsToDouble(BIGINT.getLong(block, position++));
        }

        // reconstruct the comparator and the priority queue
        this.vecDistDescComparator = new VecDistComparator(inputVec, vectorDistFuncEnum.getDistFunc());
        this.nearestVecs = new PriorityQueue<>(k, vecDistDescComparator.reversed());

        // deserialize nearest vecs, use dimension and position count
        int numNearestVecs = (int) BIGINT.getLong(block, position++);
        for (int i=0; i<numNearestVecs; i++) {
            double[] vec = new double[dimension];
            for (int d=0; d<dimension; d++) {
                vec[d] = Double.longBitsToDouble(BIGINT.getLong(block, position++));
            }
            nearestVecs.add(vec);
        }

        //todo maybe can test serialize and deserialize locally?
    }

    //todo maybe in the future would need to be more careful with this method for optimizing performance. No document describing what should be returned
    @Override
    public long getEstimatedSize() {
        return INSTANCE_SIZE + (long) dimension * 8 * nearestVecs.size();
    }

    @Override
    public int getDimension() {
        return dimension;
    }

    public int getK() {
        return k;
    }

    public double[] getInputVec() {
        return inputVec;
    }

    public VectorDistFuncs.DistFuncEnum getVectorDistFuncEnum() {
        return vectorDistFuncEnum;
    }

    public VecDistComparator getVecDistDescComparator() {
        return vecDistDescComparator;
    }

    /**
     * A vector comparator that compare two vectors based on their distance to the input vector. If a vector's distance
     * to input vector is > or < or = another vector's distance to input vector, then it returns a value >0 or =0 or <0.
     */
    public static class VecDistComparator implements Comparator<double[]>
    {
        double[] inputVec;
        VectorDistFunc vectorDistFunc;

        public VecDistComparator(double[] inputVec, VectorDistFunc vectorDistFunc) {
            this.inputVec = inputVec;
            this.vectorDistFunc = vectorDistFunc;
        }

        @Override
        public int compare(double[] v1, double[] v2) {
            double res = vectorDistFunc.getDist(v1, inputVec) - vectorDistFunc.getDist(v2, inputVec);
            if (res > 0) {
                return 1;
            } else if (res < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    }

}
