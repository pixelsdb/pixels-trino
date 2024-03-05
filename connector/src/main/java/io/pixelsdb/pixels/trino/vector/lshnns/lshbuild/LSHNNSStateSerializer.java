package io.pixelsdb.pixels.trino.vector.lshnns.lshbuild;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class LSHNNSStateSerializer implements AccumulatorStateSerializer<LSHBuildState> {
    Type doubleArrayType = new ArrayType(DOUBLE);

    @Override
    public Type getSerializedType() {
        return doubleArrayType;
    }

    @Override
    public void serialize(LSHBuildState state, BlockBuilder out) {
        state.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, LSHBuildState state) {
        doubleArrayType = new ArrayType(DOUBLE);
        state.deserialize(block.getObject(index, Block.class));

    }
}
