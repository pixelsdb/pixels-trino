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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.trino.block.BlockUtil.checkValidPositions;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The block of two children blocks, it is used to concat multiple blocks (multiple blocks can form a tree).
 * The leaf node can only be VariableWidthBlock.
 * @author hank
 * @date 2/3/23
 */
public class PairVariableWidthBlock implements Block
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(PairVariableWidthBlock.class).instanceSize());
    private final Block leftBlock;
    private final Block rightBlock;
    private final int positionCount;
    private final int leftPositionCount;

    public PairVariableWidthBlock(Block leftBlock, Block rightBlock)
    {
        requireNonNull(leftBlock, "left block is null");
        requireNonNull(rightBlock, "right block is null");
        checkArgument(leftBlock instanceof PairVariableWidthBlock || leftBlock instanceof VariableWidthBlock,
                "left block is not PairVariableWidthBlock nor VariableWidthBlock");
        checkArgument(rightBlock instanceof PairVariableWidthBlock || rightBlock instanceof VariableWidthBlock,
                "right block is not PairVariableWidthBlock nor VariableWidthBlock");
        this.leftBlock = leftBlock;
        this.rightBlock = rightBlock;
        this.leftPositionCount = leftBlock.getPositionCount();
        this.positionCount = leftBlock.getPositionCount() + rightBlock.getPositionCount();
    }

    private Block getBlock(int position)
    {
        if (position >= 0 && position < positionCount)
        {
            return position < leftPositionCount ? leftBlock : rightBlock;
        }
        throw new IndexOutOfBoundsException("position " + position + " is out of bounds 0-" + positionCount);
    }

    private int getBlockPosition(int position)
    {
        /** no need to check position boundaries again as this method is always called after {@link #getBlock(int)} */
        return position < leftPositionCount ? position : position - leftPositionCount;
    }

    public Block getLeftBlock()
    {
        return leftBlock;
    }

    public Block getRightBlock()
    {
        return rightBlock;
    }

    public int getLeftPositionCount()
    {
        return leftPositionCount;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("PairVariableWidthBlock{");
        sb.append("positionCount=").append(positionCount).append(",");
        sb.append("leftPositionCount=").append(leftPositionCount);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return getBlock(position).getSingleValueBlock(getBlockPosition(position));
    }

    @Override
    public int getSliceLength(int position)
    {
        return getBlock(position).getSliceLength(getBlockPosition(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return getBlock(position).getByte(getBlockPosition(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return getBlock(position).getShort(getBlockPosition(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return getBlock(position).getInt(getBlockPosition(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return getBlock(position).getLong(getBlockPosition(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return getBlock(position).getSlice(getBlockPosition(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return getBlock(position).getObject(getBlockPosition(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return getBlock(position).bytesEqual(getBlockPosition(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return getBlock(position).bytesCompare(getBlockPosition(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        getBlock(position).writeBytesTo(getBlockPosition(position), offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return getBlock(position).equals(getBlockPosition(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return getBlock(position).hash(getBlockPosition(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return getBlock(leftPosition).compareTo(getBlockPosition(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        return leftBlock.getLogicalSizeInBytes() + rightBlock.getLogicalSizeInBytes();
    }

    @Override
    public boolean mayHaveNull()
    {
        return leftBlock.mayHaveNull() || rightBlock.mayHaveNull();
    }

    @Override
    public boolean isLoaded()
    {
        return leftBlock.isLoaded() && rightBlock.isLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        Block leftLoaded = leftBlock.getLoadedBlock();
        Block rightLoaded = rightBlock.getLoadedBlock();

        if (leftLoaded == leftBlock && rightLoaded == rightBlock)
        {
            return this;
        }

        return new PairVariableWidthBlock(leftLoaded, rightLoaded);
    }

    @Override
    public List<Block> getChildren()
    {
        return ImmutableList.of(leftBlock, rightBlock);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return leftBlock.getSizeInBytes() + rightBlock.getSizeInBytes();
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        int leftNum = 0;
        for (int position : positions)
        {
            if (position >= 0 && position < positionCount)
            {
                if (position < leftPositionCount)
                {
                    leftNum++;
                }
            }
            else
            {
                throw new IndexOutOfBoundsException("position " + position + " is out of bounds 0-" + positionCount);
            }
        }

        int[] leftPositions = new int[leftNum];
        int[] rightPositions = new int[positions.length - leftNum];
        int leftIndex = 0, rightIndex = 0;
        for (int position : positions)
        {
            if (position < leftPositionCount)
            {
                leftPositions[leftIndex++] = position;
            }
            else
            {
                rightPositions[rightIndex++] = position - leftPositionCount;
            }
        }

        Block leftResult = leftBlock.getPositions(leftPositions, offset, length);
        Block rightResult = rightBlock.getPositions(rightPositions, offset, length);

        return new PairVariableWidthBlock(leftResult, rightResult);
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount)
        {
            throw new IndexOutOfBoundsException(format("position %s and length %s is invalid for block with %s positions",
                    positionOffset, length, positionCount));
        }
        if (positionOffset + length < leftPositionCount)
        {
            return leftBlock.getRegionSizeInBytes(positionOffset, length);
        }
        else if (positionOffset >= leftPositionCount)
        {
            return rightBlock.getRegionSizeInBytes(positionOffset - leftPositionCount, length);
        }
        else
        {
            return leftBlock.getRegionSizeInBytes(positionOffset, leftPositionCount - positionOffset) +
                    rightBlock.getRegionSizeInBytes(0, length - (leftPositionCount - positionOffset));
        }
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        OptionalInt leftFixed = leftBlock.fixedSizeInBytesPerPosition();
        OptionalInt rightFixed = rightBlock.fixedSizeInBytesPerPosition();
        if (leftFixed.equals(rightFixed))
        {
            return leftFixed;
        }
        return OptionalInt.empty();
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        checkValidPositions(positions, positionCount);
        if (selectedPositionsCount == 0)
        {
            return 0;
        }
        if (selectedPositionsCount == positionCount)
        {
            return getSizeInBytes();
        }
        OptionalInt fixedSizeInBytesPerPosition = fixedSizeInBytesPerPosition();
        if (fixedSizeInBytesPerPosition.isPresent())
        {
            // no ids repeat and the dictionary block has a fixed sizer per position
            return fixedSizeInBytesPerPosition.getAsInt() * (long) selectedPositionsCount;
        }

        boolean[] leftUsed = new boolean[leftPositionCount];
        boolean[] rightUsed = new boolean[positionCount - leftPositionCount];
        int leftSelected = 0, rightSelected = 0;
        for (int i = 0; i < leftPositionCount; ++i)
        {
            leftUsed[i] = positions[i];
            if (positions[i])
            {
                leftSelected++;
            }
        }
        for (int i = leftPositionCount; i < positionCount; ++i)
        {
            leftUsed[i-leftPositionCount] = positions[i];
            if (positions[i])
            {
                rightSelected++;
            }
        }

        return leftBlock.getPositionsSizeInBytes(leftUsed, leftSelected) +
                rightBlock.getPositionsSizeInBytes(rightUsed, rightSelected);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + leftBlock.getRetainedSizeInBytes() + rightBlock.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return getBlock(position).getEstimatedDataSizeForStats(getBlockPosition(position));
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(leftBlock, leftBlock.getRetainedSizeInBytes());
        consumer.accept(rightBlock, rightBlock.getRetainedSizeInBytes());
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return PairVariableWidthBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        int leftNum = 0;
        for (int position : positions)
        {
            if (position >= 0 && position < positionCount)
            {
                if (position < leftPositionCount)
                {
                    leftNum++;
                }
            }
            else
            {
                throw new IndexOutOfBoundsException("position " + position + " is out of bounds 0-" + positionCount);
            }
        }

        int[] leftPositions = new int[leftNum];
        int[] rightPositions = new int[positions.length - leftNum];
        int leftIndex = 0, rightIndex = 0;
        for (int position : positions)
        {
            if (position < leftPositionCount)
            {
                leftPositions[leftIndex++] = position;
            }
            else
            {
                rightPositions[rightIndex++] = position - leftPositionCount;
            }
        }

        Block leftResult = leftBlock.copyPositions(leftPositions, offset, length);
        Block rightResult = rightBlock.copyPositions(rightPositions, offset, length);

        return new PairVariableWidthBlock(leftResult, rightResult);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount)
        {
            throw new IndexOutOfBoundsException(format("position %s and length %s is invalid for block with %s positions",
                    positionOffset, length, positionCount));
        }
        if (positionOffset + length < leftPositionCount)
        {
            return leftBlock.getRegion(positionOffset, length);
        }
        else if (positionOffset >= leftPositionCount)
        {
            return rightBlock.getRegion(positionOffset - leftPositionCount, length);
        }
        else
        {
            Block leftRegion = leftBlock.getRegion(positionOffset, leftPositionCount - positionOffset);
            Block rightRegion = rightBlock.getRegion(0, length - (leftPositionCount - positionOffset));
            return new PairVariableWidthBlock(leftRegion, rightRegion);
        }
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount)
        {
            throw new IndexOutOfBoundsException(format("position %s and length %s is invalid for block with %s positions",
                    positionOffset, length, positionCount));
        }
        if (positionOffset + length < leftPositionCount)
        {
            return leftBlock.copyRegion(positionOffset, length);
        }
        else if (positionOffset >= leftPositionCount)
        {
            return rightBlock.copyRegion(positionOffset - leftPositionCount, length);
        }
        else
        {
            Block leftRegion = leftBlock.copyRegion(positionOffset, leftPositionCount - positionOffset);
            Block rightRegion = rightBlock.copyRegion(0, length - (leftPositionCount - positionOffset));
            return new PairVariableWidthBlock(leftRegion, rightRegion);
        }
    }

    @Override
    public boolean isNull(int position)
    {
        return getBlock(position).isNull(getBlockPosition(position));
    }

    @Override
    public Block copyWithAppendedNull()
    {
        return new PairVariableWidthBlock(leftBlock, rightBlock.copyWithAppendedNull());
    }
}
