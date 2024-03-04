package io.pixelsdb.pixels.trino.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;

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

    public static double[] sliceToVec(Slice slice) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            double[] vec = objectMapper.readValue(slice.toStringUtf8(), double[].class);
            return vec;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static VectorDistFuncs.DistFuncEnum sliceToDistFunc(Slice distFuncStr) {
        return switch (distFuncStr.toStringUtf8()) {
            case "euc" -> VectorDistFuncs.DistFuncEnum.EUCLIDEAN_DISTANCE;
            case "cos" -> VectorDistFuncs.DistFuncEnum.COSINE_SIMILARITY;
            case "dot" -> VectorDistFuncs.DistFuncEnum.DOT_PRODUCT;
            default -> null;
        };
    }

    public static PixelsColumnHandle sliceToColumn(Slice distFuncStr) {
       String[] schemaTableCol = distFuncStr.toStringUtf8().split("\\.");
       if (schemaTableCol.length != 3) {
           throw new IllegalColumnException("column should be of form schema.table.column");
       }
       return PixelsColumnHandle.builder()
        .setConnectorId("pixels")
        .setSchemaName(schemaTableCol[0])
        .setTableName(schemaTableCol[1])
        .setColumnName(schemaTableCol[2])
        .setColumnAlias(schemaTableCol[2])
        .setColumnType(new ArrayType(DOUBLE))
        .setTypeCategory(TypeDescription.Category.VECTOR)
        .setLogicalOrdinal(0)
        .setColumnComment("")
               .build();
    }

    public static class IllegalColumnException extends IllegalArgumentException {
        public IllegalColumnException(String msg) {
            super(msg);
        }
    }
}
