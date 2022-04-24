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

import com.google.inject.Inject;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

/**
 * Parse column type signature to Presto or Pixels data type.
 *
 * @author hank
 */
public class PixelsTypeParser
{
    private final TypeManager baseRegistry;

    @Inject
    public PixelsTypeParser(TypeManager typeRegistry)
    {
        this.baseRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
    }

    public TypeDescription parsePixelsType(String signature)
    {
        try
        {
            return TypeDescription.fromString(signature);
        }
        catch (RuntimeException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                    "failed to parse Pixels data type '" + signature + "'", e);
        }

    }

    public Type parseTrinoType(String signature)
    {
        try
        {
            return this.baseRegistry.fromSqlType(signature);
        }
        catch (RuntimeException e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                    "failed to parse Trino data type '" + signature + "'", e);
        }
    }
}