/*
 * Copyright 2025 PixelsDB.
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

import io.airlift.slice.Slices;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.trino.block.VarcharArrayBlock;
import io.pixelsdb.pixels.trino.block.VarcharArrayBlockEncoding;
import io.trino.spi.block.*;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/**
 * Lazy Block Implementation for the Pixels
 */
final class PixelsBlockLoader
        implements LazyBlockLoader
{
    /**
     *
     */
    private final PixelsPageSource pixelsPageSource;
    private final int expectedBatchId;
    private final ColumnVector vector;
    private final Type type;
    private final TypeDescription.Category typeCategory;
    private final int batchSize;

    public PixelsBlockLoader(PixelsPageSource pixelsPageSource, ColumnVector vector, Type type,
                             TypeDescription.Category typeCategory, int batchSize)
    {
        this.pixelsPageSource = pixelsPageSource;
        this.vector = requireNonNull(vector, "vector is null");
        this.type = requireNonNull(type, "type is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        this.batchSize = batchSize;
        this.expectedBatchId = this.pixelsPageSource.getBatchId();
    }

    @Override
    public Block load()
    {
        checkState(this.pixelsPageSource.getBatchId() == expectedBatchId);
        Block block;

        switch (typeCategory)
        {
            case BYTE:
                ByteColumnVector bytecv = (ByteColumnVector) vector;
                block = new ByteArrayBlock(batchSize, getNulls(bytecv), bytecv.vector);
                break;
            case SHORT:
                ShortColumnVector shortcv = (ShortColumnVector) vector;
                block = new ShortArrayBlock(batchSize, getNulls(shortcv), shortcv.vector);
                break;
            case INT:
                IntColumnVector icv = (IntColumnVector) vector;
                block = new IntArrayBlock(batchSize, getNulls(icv), icv.vector);
                break;
            case LONG:
                LongColumnVector lcv = (LongColumnVector) vector;
                block = new LongArrayBlock(batchSize, getNulls(lcv), lcv.vector);
                break;
            case DOUBLE:
                DoubleColumnVector dbcv = (DoubleColumnVector) vector;
                block = new LongArrayBlock(batchSize, getNulls(dbcv), dbcv.vector);
                break;
            case FLOAT:
                FloatColumnVector dfcv = (FloatColumnVector) vector;
                block = new IntArrayBlock(batchSize, getNulls(dfcv), dfcv.vector);
                break;
            case DECIMAL:
                /**
                 * PIXELS-196:
                 * Presto reads the unscaled values for decimal type here.
                 * The precision and scale of decimal are automatically processed by Presto.
                 */
                if (vector instanceof DecimalColumnVector dccv)
                {
                    block = new LongArrayBlock(batchSize, getNulls(dccv), dccv.vector);
                } else
                {
                    LongDecimalColumnVector ldccv = (LongDecimalColumnVector) vector;
                    block = new Int128ArrayBlock(batchSize, getNulls(ldccv), ldccv.vector);
                }
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            case VARBINARY:
                if (vector instanceof BinaryColumnVector scv)
                {
                    block = VarcharArrayBlockEncoding.Instance()
                            .replacementBlockForWrite(new VarcharArrayBlock(
                                    batchSize, scv.vector, scv.start, scv.lens, !scv.noNulls, scv.isNull)
                                    .getLoadedBlock())
                            .orElseThrow();
                }
                else
                {
                    DictionaryColumnVector dscv = (DictionaryColumnVector) vector;
                    Block dictionary = new VariableWidthBlock(dscv.dictOffsets.length - 1,
                            Slices.wrappedBuffer(dscv.dictArray), dscv.dictOffsets, Optional.empty());
                    if (!dscv.noNulls)
                    {
                        // Issue #84: Trino's stupid DictionaryBlock stores null value in dictionary.
                        int nullValueId = dictionary.getPositionCount();
                        dictionary = dictionary.copyWithAppendedNull();
                        for (int i = 0; i < batchSize; ++i)
                        {
                            if (dscv.isNull[i])
                            {
                                dscv.ids[i] = nullValueId;
                            }
                        }
                    }
                    block = DictionaryBlock.create(batchSize, dictionary, dscv.ids);
                }
                break;
            case BOOLEAN:
                ByteColumnVector bcv = (ByteColumnVector) vector;
                block = new ByteArrayBlock(batchSize, getNulls(bcv), bcv.vector);
                break;
            case DATE:
                // PIXELS-94: add date type.
                DateColumnVector dtcv = (DateColumnVector) vector;
                // In pixels and Presto, date is stored as the number of days from UTC 1970-1-1 0:0:0.
                block = new IntArrayBlock(batchSize, getNulls(dtcv), dtcv.dates);
                break;
            case TIME:
                // TIME is read as LongTimeColumnVector (picoseconds of day) for zero-copy LongArrayBlock.
                LongTimeColumnVector tcv = (LongTimeColumnVector) vector;
                block = new LongArrayBlock(batchSize, getNulls(tcv), tcv.vector);
                break;
            case TIMESTAMP:
                TimestampColumnVector tscv = (TimestampColumnVector) vector;
                /**
                 * PIXELS-94: we have confirmed that LongArrayBlock is used for timestamp
                 * type in Presto.
                 *
                 * io.trino.spi.type.TimestampType extends
                 * io.trino.spi.type.AbstractLongType, which creates a LongArrayBlockBuilder.
                 * And this block builder builds a LongArrayBlock.
                 */
                block = new LongArrayBlock(batchSize, getNulls(tscv), tscv.times);
                break;
            case VECTOR:
                VectorColumnVector vcv = (VectorColumnVector) vector;
                // builder that simply concatenate all double arrays
                BlockBuilder allDoublesBuilder = DOUBLE.createBlockBuilder(null, batchSize * vcv.dimension);
                int[] offsets = new int[batchSize + 1];
                // build a block into which we put a double array
                for (int i = 0; i < batchSize; i++)
                {
                    offsets[i] = i * vcv.dimension;
                    for (int j = 0; j < vcv.dimension; j++)
                    {
                        DOUBLE.writeDouble(allDoublesBuilder, vcv.vector[i][j]);
                    }
                }
                offsets[batchSize] = batchSize * vcv.dimension;
                // after extensive research on how other connectors deal with array type, the following seems to
                // be the way to go: basically we stuff all the values of all arrays into one big block, and provide
                // an int[] as offsets to tell trino where each array begins and ends. Note that the final offset
                // should be the position to tell trino the end of the final array
                // Interestingly all the above is NOT documented in trino documentation or code at all.
                block = ArrayBlock.fromElementBlock(
                        batchSize, getNulls(vcv), offsets, allDoublesBuilder.build());
                break;
            default:
                BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize);
                for (int i = 0; i < batchSize; ++i)
                {
                    blockBuilder.appendNull();
                }
                block = blockBuilder.build();
                break;
        }

        return block;
    }

    private static Optional<boolean[]> getNulls(ColumnVector vector)
    {
        return vector.noNulls ? Optional.empty() : Optional.of(vector.isNull);
    }
}