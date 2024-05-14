/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino.vector.exactnns;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class ExactNNSStateSerializer implements AccumulatorStateSerializer<ExactNNSState>
{
    Type doubleArrayType = new ArrayType(DOUBLE);

    @Override
    public Type getSerializedType()
    {
        return doubleArrayType;
    }

    @Override
    public void serialize(ExactNNSState state, BlockBuilder out)
    {
        state.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, ExactNNSState state)
    {
        doubleArrayType = new ArrayType(DOUBLE);
        state.deserialize(block.getObject(index, Block.class));
    }
}
