package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Arrays;

public class LSHLoadUDF {
    @ScalarFunction("lsh_load")
    @Description("load data and build lsh index on the target column")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice lshLoad(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice filesToLoadSlice,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice schemaTableColumnSlice,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice tableS3Path,
            @SqlType("integer") long numBits)
    {
        String[] filesToLoad = filesToLoadSlice.toStringUtf8().split("//s*,//s*");
        LSHLoader lshLoader = new LSHLoader(filesToLoad, schemaTableColumnSlice.toStringUtf8(), tableS3Path.toStringUtf8(), (int)numBits);
        lshLoader.load();
        return Slices.utf8Slice(Arrays.toString(filesToLoad) + " have been written to " + tableS3Path);
    }
}
