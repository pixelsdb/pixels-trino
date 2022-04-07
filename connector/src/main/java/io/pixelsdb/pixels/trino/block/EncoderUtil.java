/*
 * Copyright 2019 PixelsDB.
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
import io.trino.spi.block.Block;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * This class is copied from io.trino.spi.block.EncoderUtil, because it's not public.
 * Created at: 19-6-1
 * Author: hank
 */
final class EncoderUtil
{
    private EncoderUtil()
    {
    }

    /**
     * Append null values for the block as a stream of bits.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "ImplicitNumericConversion"})
    public static void encodeNullsAsBits(SliceOutput sliceOutput, Block block)
    {
        boolean mayHaveNull = block.mayHaveNull();
        sliceOutput.writeBoolean(mayHaveNull);
        if (!mayHaveNull) {
            return;
        }

        int positionCount = block.getPositionCount();
        byte[] packedIsNull = new byte[((positionCount & ~0b111) + 1) / 8];
        int currentByte = 0;

        for (int position = 0; position < (positionCount & ~0b111); position += 8, currentByte++) {
            byte value = 0;
            value |= block.isNull(position) ? 0b1000_0000 : 0;
            value |= block.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= block.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= block.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= block.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= block.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= block.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= block.isNull(position + 7) ? 0b0000_0001 : 0;
            packedIsNull[currentByte] = value;
        }

        sliceOutput.writeBytes(packedIsNull);

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                value |= block.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount)
    {
        return Optional.ofNullable(retrieveNullBits(sliceInput, positionCount))
                .map(packedIsNull -> decodeNullBits(packedIsNull, positionCount));
    }

    public static boolean[] decodeNullBits(byte[] packedIsNull, int positionCount)
    {
        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        int currentByte = 0;
        for (int position = 0; position < (positionCount & ~0b111); position += 8, currentByte++) {
            byte value = packedIsNull[currentByte];
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = packedIsNull[packedIsNull.length - 1];
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        return valueIsNull;
    }

    @Nullable
    public static byte[] retrieveNullBits(SliceInput sliceInput, int positionCount)
    {
        if (!sliceInput.readBoolean()) {
            return null;
        }
        try {
            return sliceInput.readNBytes((positionCount + 7) / 8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

