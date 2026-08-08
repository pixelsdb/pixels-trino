/*
 * Copyright 2021 PixelsDB.
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

import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ValueBlock;
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.pixelsdb.pixels.trino.block.BlockUtil.*;

/**
 * This class is derived from io.trino.spi.block.IntArrayBlock.
 *
 * With this class, we use int values to simulate a LongArrayBlock, so that
 * we can reduce 50% memory footprint. Int value is enough for time type
 * in Pixels.
 *
 * Modifications:
 * 1. add getLong, getShort, getByte, so that this class can be compatible
 * with io.trino.spi.block.LongArrayBlock.
 *
 * 2. change the returned statement of the methods that return Block or
 * BlockEncoding.
 *
 * @author hank
 * @create 2021-04-26
 * @update 2024-12-01 adapt to with Trino 465 and add hasNull argument to the constructor.
 */
public class TimeArrayBlock implements ValueBlock
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(TimeArrayBlock.class).instanceSize();
    public static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;
    /**
     * Trino assumes each time value is a long of precision 12,
     * so we need to multiply a scale factor for each value.
     */
    private static final long SCALE_FACTOR = 1000000000L;

    private final int arrayOffset;
    private final int positionCount;
    private final int[] values;
    private final boolean[] valueIsNull;
    private final boolean hasNull;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public TimeArrayBlock(int positionCount, int[] values, boolean hasNull, boolean[] valueIsNull)
    {
        this(0, positionCount, values, hasNull, valueIsNull);
    }

    TimeArrayBlock(int arrayOffset, int positionCount, int[] values, boolean hasNull, boolean[] valueIsNull)
    {
        if (arrayOffset < 0)
        {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0)
        {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values == null || values.length - arrayOffset < positionCount)
        {
            throw new IllegalArgumentException("values is null or its length is less than positionCount");
        }
        this.values = values;

        this.hasNull = hasNull;
        // Issue #123: in Pixels, the isNull bitmap from column vectors always presents even if there is no nulls.
        if (valueIsNull == null || valueIsNull.length - arrayOffset < positionCount)
        {
            throw new IllegalArgumentException("valueIsNull is null or its length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        return (long) SIZE_IN_BYTES_PER_POSITION * selectedPositionsCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    /**
     * Returns the estimated in memory data size for stats of position.
     * Do not use it for other purpose.
     *
     * @param position
     */
    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Integer.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    public long getLong(int position)
    {
        checkReadablePosition(position);
        return values[position + arrayOffset] * SCALE_FACTOR;
    }

    public int getInt(int position)
    {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    protected int[] getRawValues()
    {
        return this.values;
    }

    protected int getRawValuesOffset()
    {
        return this.arrayOffset;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return hasNull && valueIsNull[position + arrayOffset];
    }

    /**
     * Returns a block that contains a copy of the contents of the current block, and an appended null at the end. The
     * original block will not be modified. The purpose of this method is to leverage the contents of a block and the
     * structure of the implementation to efficiently produce a copy of the block with a NULL element inserted - so that
     * it can be used as a dictionary. This method is expected to be invoked on completely built {@link Block} instances
     * i.e. not on in-progress block builders.
     */
    @Override
    public TimeArrayBlock copyWithAppendedNull()
    {
        boolean[] newValueIsNull = copyIsNullAndAppendNull(valueIsNull, arrayOffset, positionCount);
        int[] newValues = ensureCapacity(values, arrayOffset + positionCount + 1);

        return new TimeArrayBlock(arrayOffset, positionCount + 1, newValues, true, newValueIsNull);
    }

    @Override
    public ValueBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public int getUnderlyingValuePosition(int position)
    {
        return position;
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNull;
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(valueIsNull, arrayOffset, positionCount);
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        return ValueBlock.super.getPositions(positions, offset, length);
    }

    @Override
    public boolean isLoaded()
    {
        return true;
    }

    @Override
    public Block getLoadedBlock()
    {
        return this;
    }

    @Override
    public TimeArrayBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new TimeArrayBlock(1,
                new int[] {values[position + arrayOffset]},
                hasNull && valueIsNull[position + arrayOffset],
                new boolean[] {valueIsNull[position + arrayOffset]});
    }

    @Override
    public TimeArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = new boolean[length];
        boolean newHasNull = false;
        int[] newValues = new int[length];
        for (int i = 0; i < length; i++)
        {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNull && valueIsNull[position + arrayOffset])
            {
                newValueIsNull[i] = true;
                newHasNull = true;
            }
            else
            {
                newValues[i] = values[position + arrayOffset];
            }
        }
        return new TimeArrayBlock(length, newValues, newHasNull, newValueIsNull);
    }

    @Override
    public TimeArrayBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        boolean newHasNull = false;
        if (hasNull)
        {
            for (int i = 0; i < length; ++i)
            {
                if (valueIsNull[i + arrayOffset])
                {
                    newHasNull = true;
                    break;
                }
            }
        }
        return new TimeArrayBlock(positionOffset + arrayOffset, length, values, newHasNull, valueIsNull);
    }

    @Override
    public TimeArrayBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        boolean[] newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        int[] newValues = compactArray(values, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values)
        {
            return this;
        }

        boolean newHasNull = false;
        if (hasNull)
        {
            for (int i = 0; i < length; ++i)
            {
                if (newValueIsNull[i])
                {
                    newHasNull = true;
                    break;
                }
            }
        }
        return new TimeArrayBlock(length, newValues, newHasNull, newValueIsNull);
    }

    @Override
    public String getEncodingName()
    {
        return TimeArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TimeArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount())
        {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
