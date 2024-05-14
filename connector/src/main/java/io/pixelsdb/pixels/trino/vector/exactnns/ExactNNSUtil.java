/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.trino.vector.exactnns;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;
import io.trino.spi.block.Block;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class ExactNNSUtil
{
    public static double[] blockToVec(Block block)
    {
        double[] inputVector;
        if (block == null)
        {
            return null;
        }
        // todo use offset here
        inputVector = new double[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++)
        {
            inputVector[i] = DOUBLE.getDouble(block, i);
        }
        return inputVector;
    }

    public static VectorDistFuncs.DistFuncEnum sliceToDistFunc(Slice distFuncStr)
    {
        return switch (distFuncStr.toStringUtf8())
        {
            case "euc" -> VectorDistFuncs.DistFuncEnum.EUCLIDEAN_DISTANCE;
            case "cos" -> VectorDistFuncs.DistFuncEnum.COSINE_SIMILARITY;
            case "dot" -> VectorDistFuncs.DistFuncEnum.DOT_PRODUCT;
            default -> null;
        };
    }
}
