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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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

    private static VariableWidthBlockEncoding LeafNodeEncoding = new VariableWidthBlockEncoding();

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

        // For pair node, we write the position count and the left position count.
        sliceOutput.appendInt(pairBlock.getPositionCount());
        sliceOutput.appendInt(pairBlock.getLeftPositionCount());

        Block leftBlock = pairBlock.getLeftBlock();
        Block rightBlock = pairBlock.getRightBlock();
        if (leftBlock instanceof VariableWidthBlock)
        {
            sliceOutput.appendByte(0); // 0 means this is a leaf node.
            LeafNodeEncoding.writeBlock(blockEncodingSerde, sliceOutput, leftBlock);
        }
        else
        {
            sliceOutput.appendByte(1); // 1 means this is a pair node.
            this.writeBlock(blockEncodingSerde, sliceOutput, leftBlock);
        }
        if (rightBlock instanceof VariableWidthBlock)
        {
            sliceOutput.appendByte(0); // 0 means this is a leaf node.
            LeafNodeEncoding.writeBlock(blockEncodingSerde, sliceOutput, rightBlock);
        }
        else
        {
            sliceOutput.appendByte(1); // 1 means this is a pair node.
            this.writeBlock(blockEncodingSerde, sliceOutput, rightBlock);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        int leftPositionCount = sliceInput.readInt();
        Block leftNode, rightNode;
        byte isPairNode = sliceInput.readByte();
        if (isPairNode == 0)
        {
            // this is a leaf node
            leftNode = LeafNodeEncoding.readBlock(blockEncodingSerde, sliceInput);
        }
        else
        {
            leftNode = this.readBlock(blockEncodingSerde, sliceInput);
        }
        isPairNode = sliceInput.readByte();
        if (isPairNode == 0)
        {
            // this is a leaf node
            rightNode = LeafNodeEncoding.readBlock(blockEncodingSerde, sliceInput);
        }
        else
        {
            rightNode = this.readBlock(blockEncodingSerde, sliceInput);
        }
        requireNonNull(leftNode, "the decoded left node is null");
        requireNonNull(rightNode, "the decoded right node is null");
        checkArgument(leftNode.getPositionCount() == leftPositionCount,
                "the decoded left node has an incorrect position");
        checkArgument(leftNode.getPositionCount() + rightNode.getPositionCount() == positionCount,
                "the decoded right node has an incorrect position count");
        return new PairVariableWidthBlock(leftNode, rightNode);
    }
}
