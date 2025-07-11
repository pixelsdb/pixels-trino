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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlock;

import java.util.Optional;

import static io.pixelsdb.pixels.trino.block.EncoderUtil.decodeNullBits;
import static io.pixelsdb.pixels.trino.block.EncoderUtil.encodeNullsAsBits;

/**
 * This class is derived from io.trino.spi.block.VariableWidthBlockEncoding
 *
 * We reimplemented writeBlock and readBlock
 *
 * @author hank
 */
public class VarcharArrayBlockEncoding implements BlockEncoding
{
    public static final String NAME = "VARCHAR_ARRAY";

    private static final VarcharArrayBlockEncoding instance = new VarcharArrayBlockEncoding();

    public static VarcharArrayBlockEncoding Instance()
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
        VarcharArrayBlock varcharArrayBlock = (VarcharArrayBlock) block;

        int positionCount = varcharArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);
        // hasNull
        sliceOutput.appendByte(varcharArrayBlock.mayHaveNull() ? 1 : 0);

        // do not encode offsets, they should be 0.

        // lengths
        for (int position = 0; position < positionCount; position++)
        {
            sliceOutput.appendInt(varcharArrayBlock.getSliceLength(position));
        }

        // isNull
        encodeNullsAsBits(sliceOutput, varcharArrayBlock);

        // values
        // sliceOutput.appendInt((int) varcharArrayBlock.getSizeInBytes());
        for (int position = 0; position < positionCount; position++)
        {
            byte[] rawValue = varcharArrayBlock.getRawValue(position);
            if (rawValue != null)
            {
                sliceOutput.writeBytes(rawValue, varcharArrayBlock.getPositionOffset(position),
                        varcharArrayBlock.getSliceLength(position));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        boolean hasNull = sliceInput.readByte() != 0;
        int[] offsets = new int[positionCount];
        int[] lengths = new int[positionCount];

        // offsets should be 0, do not read them from sliceInput.

        // destinationIndex should be 0, because we do not need 0 to be the first item in lengths.
        sliceInput.readInts(lengths, 0, positionCount);

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(new boolean[positionCount]);

        // int blockSize = sliceInput.readInt();
        byte[][] values = new byte[positionCount][];
        for (int position = 0; position < positionCount; position++)
        {
            values[position] = new byte[lengths[position]];
            sliceInput.readBytes(values[position]);
        }

        return new VarcharArrayBlock(positionCount, values, offsets, lengths, hasNull, valueIsNull);
    }

    /**
     *
     * ISSUE-902: Trino expects standard block types like {@code VariableWidthBlock} for
     * Varchar values when serializing query results. However, {@code VarcharArrayBlock}
     * causes {@link ClassCastException} in paths such as {@code JsonEncodingUtils}. This method provides
     * a fallback replacement to ensure compatibility by converting the {@code VarcharArrayBlock} into a supported format.
     *
     * @param block
     * @return
     */
    @Override
    public Optional<Block> replacementBlockForWrite(Block block)
    {
        if (!(block instanceof VarcharArrayBlock varcharBlock)) {
            return Optional.empty();
        }

        int positionCount = varcharBlock.getPositionCount();

        int totalLength = 0;
        for (int i = 0; i < positionCount; ++i) {
            totalLength += varcharBlock.getSliceLength(i);
        }

        byte[] content = new byte[totalLength];
        int[] offsets = new int[positionCount + 1];
        int curOffset = 0;

        for (int i = 0; i < positionCount; ++i) {
            offsets[i] = curOffset;
            int len = varcharBlock.getSliceLength(i);
            if (!varcharBlock.isNull(i)) {
                System.arraycopy(
                        varcharBlock.getRawValue(i),
                        varcharBlock.getPositionOffset(i),
                        content,
                        curOffset,
                        len
                );
            }
            curOffset += len;
        }
        offsets[positionCount] = totalLength;

        VariableWidthBlock newBlock = new VariableWidthBlock(
                positionCount,
                Slices.wrappedBuffer(content),
                offsets,
                Optional.of(varcharBlock.getValueIsNull())
        );

        return Optional.of(newBlock);
    }
}
