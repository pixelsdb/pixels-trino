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
package io.pixelsdb.pixels.trino;

import org.junit.Test;

import java.math.BigInteger;
import java.sql.Timestamp;

public class TestString
{
    @Test
    public void test ()
    {
        StringBuilder sb = new StringBuilder("<");
        for (int i = 0; i < 5; i++)
        {
            sb.append(":,");
        }
        sb.replace(sb.length() - 1, sb.length(), ">");
        System.out.println(sb.toString());
    }

    @Test
    public void test1()
    {
        BigInteger min = BigInteger.valueOf(Long.MIN_VALUE).shiftLeft(64);
        BigInteger max = BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(64).add(BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(1).add(BigInteger.ONE));
        System.out.println(min);
        System.out.println(max);
        System.out.println(min.shiftRight(64).longValueExact());
        System.out.println(min.longValue());
        System.out.println(max.shiftRight(64).longValueExact());
        System.out.println(max.longValue());

        System.out.println(1234%1000*1000);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp);
        System.out.println(timestamp.getTime());
        System.out.println(timestamp.getNanos());
        System.out.println(timestamp.getTime()*1000 + timestamp.getNanos()%1000000/1000);
        long time = timestamp.getTime()*1000 + timestamp.getNanos()%1000000/1000;
        timestamp.setTime(time/1000);
        timestamp.setNanos((int) (time%1000000*1000));
        System.out.println(timestamp);

        String a = "a|b|c";
        String[] split = a.split("\\|");
        System.out.println(split.length);
    }
}
