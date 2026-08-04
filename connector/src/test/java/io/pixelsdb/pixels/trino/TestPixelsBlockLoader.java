/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPixelsBlockLoader
{
    @Test
    public void testTinyintBlock()
    {
        byte[] values = {-128, -9, 0, 8, 127};
        ByteColumnVector vector = new ByteColumnVector(values.length + 1);
        for (byte value : values)
        {
            vector.add(value);
        }
        vector.addNull();

        Block block = load(vector, TinyintType.TINYINT, TypeDescription.Category.BYTE, values.length + 1);

        assertTrue(block instanceof ByteArrayBlock);
        for (int i = 0; i < values.length; ++i)
        {
            assertEquals((long) values[i], TinyintType.TINYINT.getLong(block, i));
        }
        assertTrue(block.isNull(values.length));
    }

    @Test
    public void testSmallintBlock()
    {
        int[] values = {Short.MIN_VALUE, -3456, 0, 1234, Short.MAX_VALUE};
        IntColumnVector vector = new IntColumnVector(values.length + 1);
        for (int value : values)
        {
            vector.add(value);
        }
        vector.addNull();

        Block block = load(vector, SmallintType.SMALLINT, TypeDescription.Category.SHORT, values.length + 1);

        assertTrue(block instanceof ShortArrayBlock);
        for (int i = 0; i < values.length; ++i)
        {
            assertEquals((long) values[i], SmallintType.SMALLINT.getLong(block, i));
        }
        assertTrue(block.isNull(values.length));
    }

    @Test
    public void testSmallintRejectsOutOfRangeValue()
    {
        IntColumnVector vector = new IntColumnVector(1);
        vector.add(Short.MAX_VALUE + 1);

        assertThrows(IllegalArgumentException.class,
                () -> load(vector, SmallintType.SMALLINT, TypeDescription.Category.SHORT, 1));
    }

    @Test
    public void testTimeBlock()
    {
        int[] millis = {0, 3_723_004, 86_399_999};
        TimeColumnVector vector = new TimeColumnVector(millis.length + 1, 3);
        for (int i = 0; i < millis.length; ++i)
        {
            vector.set(i, millis[i]);
        }
        vector.setNullValue(millis.length);

        Block block = load(vector, TimeType.TIME_MILLIS, TypeDescription.Category.TIME, millis.length + 1);

        assertTrue(block instanceof LongArrayBlock);
        for (int i = 0; i < millis.length; ++i)
        {
            assertEquals((long) millis[i] * 1_000_000_000L, TimeType.TIME_MILLIS.getLong(block, i));
        }
        assertTrue(block.isNull(millis.length));
    }

    @Test
    public void testVarcharBlock()
    {
        // Capacity larger than batchSize: only the logical batch must be emitted.
        int batchSize = 7;
        BinaryColumnVector vector = new BinaryColumnVector(batchSize + 4);

        // Shared backing buffer with non-zero starts.
        byte[] shared = "XXXhelloYYYY中文ZZ😀WW".getBytes(StandardCharsets.UTF_8);
        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] chinese = "中文".getBytes(StandardCharsets.UTF_8);
        byte[] emoji = "😀".getBytes(StandardCharsets.UTF_8);

        int helloStart = indexOf(shared, hello);
        int chineseStart = indexOf(shared, chinese);
        int emojiStart = indexOf(shared, emoji);

        vector.setRef(0, shared, helloStart, hello.length);
        vector.setRef(1, shared, chineseStart, chinese.length);
        vector.setRef(2, shared, emojiStart, emoji.length);
        vector.setRef(3, new byte[0], 0, 0); // empty string (non-null)
        // NULL with intentionally stale start/lens/vector that must be ignored.
        vector.addNull();
        assertEquals(5, vector.getWriteIndex());
        vector.vector[4] = shared;
        vector.start[4] = helloStart;
        vector.lens[4] = hello.length;
        byte[] afterNull = "after-null".getBytes(StandardCharsets.UTF_8);
        byte[] ascii = "ascii".getBytes(StandardCharsets.UTF_8);
        vector.setRef(5, afterNull, 0, afterNull.length);
        vector.setRef(6, ascii, 0, ascii.length);
        // Extra capacity row must not appear in the block.
        vector.setRef(7, "ignored".getBytes(StandardCharsets.UTF_8), 0, 7);

        Block block = load(vector, VarcharType.VARCHAR, TypeDescription.Category.VARCHAR, batchSize);

        assertTrue(block instanceof VariableWidthBlock);
        VariableWidthBlock vw = (VariableWidthBlock) block;
        assertEquals(batchSize, vw.getPositionCount());

        assertSliceEquals(VarcharType.VARCHAR, vw, 0, hello);
        assertSliceEquals(VarcharType.VARCHAR, vw, 1, chinese);
        assertSliceEquals(VarcharType.VARCHAR, vw, 2, emoji);
        assertFalse(vw.isNull(3));
        assertEquals(0, vw.getSliceLength(3));
        assertSliceEquals(VarcharType.VARCHAR, vw, 3, new byte[0]);
        assertTrue(vw.isNull(4));
        assertEquals(0, vw.getSliceLength(4));
        assertSliceEquals(VarcharType.VARCHAR, vw, 5, afterNull);
        assertSliceEquals(VarcharType.VARCHAR, vw, 6, ascii);
    }

    @Test
    public void testVarbinaryBlock()
    {
        int batchSize = 4;
        BinaryColumnVector vector = new BinaryColumnVector(batchSize + 2);

        byte[] raw = {(byte) 0x00, (byte) 0x7F, (byte) 0x80, (byte) 0xFE, (byte) 0xFF};
        byte[] high = {(byte) 0xFF, (byte) 0x00, (byte) 0xAB};
        byte[] empty = new byte[0];

        vector.setRef(0, raw, 0, raw.length);
        vector.setRef(1, high, 0, high.length);
        vector.setRef(2, empty, 0, 0);
        vector.addNull();
        // Stale payload on the NULL row.
        vector.vector[3] = raw;
        vector.start[3] = 1;
        vector.lens[3] = 3;
        vector.setRef(4, new byte[] {(byte) 0x11}, 0, 1);

        Block block = load(vector, VarbinaryType.VARBINARY, TypeDescription.Category.VARBINARY, batchSize);

        assertTrue(block instanceof VariableWidthBlock);
        VariableWidthBlock vw = (VariableWidthBlock) block;
        assertEquals(batchSize, vw.getPositionCount());
        assertSliceEquals(VarbinaryType.VARBINARY, vw, 0, raw);
        assertSliceEquals(VarbinaryType.VARBINARY, vw, 1, high);
        assertFalse(vw.isNull(2));
        assertEquals(0, vw.getSliceLength(2));
        assertSliceEquals(VarbinaryType.VARBINARY, vw, 2, empty);
        assertTrue(vw.isNull(3));
        assertEquals(0, vw.getSliceLength(3));
    }

    private static void assertSliceEquals(Type type, Block block, int position, byte[] expected)
    {
        assertFalse(block.isNull(position));
        assertArrayEquals(expected, type.getSlice(block, position).getBytes());
    }

    private static int indexOf(byte[] haystack, byte[] needle)
    {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; ++i)
        {
            for (int j = 0; j < needle.length; ++j)
            {
                if (haystack[i + j] != needle[j])
                {
                    continue outer;
                }
            }
            return i;
        }
        throw new IllegalArgumentException("needle not found in haystack");
    }

    private static Block load(ColumnVector vector, Type type,
                              TypeDescription.Category typeCategory, int batchSize)
    {
        return new PixelsBlockLoader(new TestPageSource(), vector, type, typeCategory, batchSize).load();
    }

    private static class TestPageSource implements PixelsPageSource
    {
        @Override
        public int getBatchId()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public Page getNextPage()
        {
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    }
}
