/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.trino.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.*;

/**
 * We reimplemented writeBlock and readBlock
 *
 * @author hank
 */
public class PairVariableWidthBlockEncoding implements BlockEncoding
{
    public static final String NAME = "PAIR_VARIABLE_WIDTH";

    private static final PairVariableWidthBlockEncoding instance = new PairVariableWidthBlockEncoding();

    public static PairVariableWidthBlockEncoding Instance()
    {
        return instance;
    }

    private VariableWidthBlockEncoding variableWidthBlockEncoding = new VariableWidthBlockEncoding();

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        PairVariableWidthBlock pairBlock = (PairVariableWidthBlock) block;

        sliceOutput.appendInt(pairBlock.getPositionCount());
        sliceOutput.appendInt(pairBlock.getLeftPositionCount());

        Block leftBlock = pairBlock.getLeftBlock();
        Block rightBlock = pairBlock.getRightBlock();
        if (leftBlock instanceof VariableWidthBlock)
        {
            //((VariableWidthBlock) leftBlock).
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        return null;
    }
}
