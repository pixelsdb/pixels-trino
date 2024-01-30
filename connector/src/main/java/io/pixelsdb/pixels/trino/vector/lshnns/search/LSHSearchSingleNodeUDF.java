package io.pixelsdb.pixels.trino.vector.lshnns.search;

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class LSHSearchSingleNodeUDF {
    private LSHSearchSingleNodeUDF() {}

    @ScalarFunction("lsh_search_single_node")
    @Description("calculate the Euclidean distance between two vectors")
    @SqlType(StandardTypes.JSON)
    @SqlNullable
    public static Double eucDist(
            @SqlNullable @SqlType("array(double)") Block inputVec,
            @SqlNullable @SqlType("array(double)") Block vec2)
    {
        return 0.0;
    }
}
