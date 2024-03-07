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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.pixelsdb.pixels.trino.block.TimeArrayBlockEncoding;
import io.pixelsdb.pixels.trino.block.VarcharArrayBlockEncoding;
import io.pixelsdb.pixels.trino.vector.VectorUDF;
import io.pixelsdb.pixels.trino.vector.exactnns.ExactNNSAggFunc;
import io.pixelsdb.pixels.trino.vector.lshnns.lshbuild.BuildLSHIndexAggFunc;
import io.pixelsdb.pixels.trino.vector.lshnns.lshbuild.LSHLoadUDF;
import io.pixelsdb.pixels.trino.vector.lshnns.search.LSHSearchUDF;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Set;

public class PixelsPlugin implements Plugin
{
    @Override
    public Iterable<BlockEncoding> getBlockEncodings()
    {
        return ImmutableList.of(VarcharArrayBlockEncoding.Instance(), TimeArrayBlockEncoding.Instance());
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new PixelsConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(VectorUDF.class)
                .add(ExactNNSAggFunc.class)
                .add(BuildLSHIndexAggFunc.class)
                .add(LSHSearchUDF.class)
                .add(LSHLoadUDF.class)
                .build();
    }
}
