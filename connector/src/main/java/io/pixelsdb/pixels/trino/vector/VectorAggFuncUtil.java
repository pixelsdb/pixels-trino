package io.pixelsdb.pixels.trino.vector;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class VectorAggFuncUtil {

    public static double[] blockToVec(Block block) {
        double[] inputVector;
        if (block == null) {
            return null;
        }
        // todo use offset here
        inputVector = new double[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            inputVector[i] = DOUBLE.getDouble(block, i);
        }
        return inputVector;
    }

    public static VectorDistFuncs.DistFuncEnum sliceToDistFunc(Slice distFuncStr) {
        return switch (distFuncStr.toStringUtf8()) {
            case "euc" -> VectorDistFuncs.DistFuncEnum.EUCLIDEAN_DISTANCE;
            case "cos" -> VectorDistFuncs.DistFuncEnum.COSINE_SIMILARITY;
            case "dot" -> VectorDistFuncs.DistFuncEnum.DOT_PRODUCT;
            default -> null;
        };
    }
}
