/*
 * Copyright 2022 PixelsDB.
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
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.pixelsdb.pixels.trino.block.EncoderUtil.decodeNullBits;
import static io.pixelsdb.pixels.trino.block.EncoderUtil.encodeNullsAsBits;

/**
 * This class is derived from io.trino.spi.block.IntArrayBlockEncoding.
 *
 * @author hank
 */
public class TimeArrayBlockEncoding implements BlockEncoding
{
    public static final String NAME = "TIME_ARRAY";

    private static final TimeArrayBlockEncoding instance = new TimeArrayBlockEncoding();

    public static TimeArrayBlockEncoding Instance()
    {
        return instance;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        TimeArrayBlock timeArrayBlock = (TimeArrayBlock) block;

        int positionCount = timeArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);
        // hasNull
        sliceOutput.appendByte(timeArrayBlock.mayHaveNull() ? 1 : 0);

        encodeNullsAsBits(sliceOutput, timeArrayBlock);

        if (!timeArrayBlock.mayHaveNull())
        {
            sliceOutput.writeInts(timeArrayBlock.getRawValues(),
                    timeArrayBlock.getRawValuesOffset(), timeArrayBlock.getPositionCount());
        }
        else
        {
            for (int position = 0; position < positionCount; position++)
            {
                if (!timeArrayBlock.isNull(position))
                {
                    sliceOutput.writeInt(timeArrayBlock.getInt(position));
                }
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        boolean hasNull = sliceInput.readByte() != 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).get();

        int[] values = new int[positionCount];
        for (int position = 0; position < positionCount; position++)
        {
            if (!valueIsNull[position])
            {
                values[position] = sliceInput.readInt();
            }
        }

        return new TimeArrayBlock(positionCount, values, hasNull, valueIsNull);
    }
}
