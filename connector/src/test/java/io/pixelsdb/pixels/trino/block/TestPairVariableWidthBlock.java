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

import io.airlift.slice.*;
import io.trino.spi.block.VariableWidthBlock;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * Created at: 04/02/2023
 * Author: hank
 */
public class TestPairVariableWidthBlock
{
    @Test
    public void testEncoding() throws IOException
    {
        VariableWidthBlock leaf1 = new VariableWidthBlock(3, Slices.utf8Slice("123456789"),
                new int[] {0, 3, 6, 9}, Optional.empty());
        VariableWidthBlock leaf2 = new VariableWidthBlock(3, Slices.utf8Slice("abcdefghi"),
                new int[] {0, 3, 6, 9}, Optional.empty());
        PairVariableWidthBlock parent1 = new PairVariableWidthBlock(leaf1, leaf2);
        VariableWidthBlock leaf3 = new VariableWidthBlock(3, Slices.utf8Slice("qwertyuio"),
                new int[] {0, 3, 6, 9}, Optional.empty());
        PairVariableWidthBlock parent2 = new PairVariableWidthBlock(parent1, leaf3);

        PairVariableWidthBlockEncoding encoding = new PairVariableWidthBlockEncoding();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
        SliceOutput sliceOutput = new OutputStreamSliceOutput(outputStream);
        encoding.writeBlock(null, sliceOutput, parent2);
        sliceOutput.flush();
        sliceOutput.close();

        byte[] backingBytes = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(backingBytes);
        SliceInput sliceInput = new InputStreamSliceInput(inputStream);
        PairVariableWidthBlock parent3 = (PairVariableWidthBlock) encoding.readBlock(null, sliceInput);
        sliceInput.close();
        assert parent3.getPositionCount() == parent2.getPositionCount();
        assert parent3.getLeftPositionCount() == parent2.getLeftPositionCount();
        for (int i = 0; i < parent3.getPositionCount(); ++i)
        {
            assert parent3.equals(i, 0, parent2, i, 0, 3);
        }
    }
}
