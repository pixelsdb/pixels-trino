package io.pixelsdb.pixels.trino;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

import java.util.Optional;

import io.airlift.slice.Slices;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.trino.block.TimeArrayBlock;
import io.pixelsdb.pixels.trino.block.VarcharArrayBlock;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;

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
            case SHORT:
            case INT:
                IntColumnVector icv = (IntColumnVector) vector;
                block = new IntArrayBlock(batchSize, Optional.ofNullable(icv.isNull), icv.vector);
                break;
            case LONG:
                LongColumnVector lcv = (LongColumnVector) vector;
                block = new LongArrayBlock(batchSize, Optional.ofNullable(lcv.isNull), lcv.vector);
                break;
            case DOUBLE:
            case FLOAT:
                /**
                 * According to TypeDescription.createColumn(),
                 * both float and double type use DoubleColumnVector, while they use
                 * FloatColumnReader and DoubleColumnReader respectively according to
                 * io.pixelsdb.pixels.reader.ColumnReader.newColumnReader().
                 */
                DoubleColumnVector dbcv = (DoubleColumnVector) vector;
                block = new LongArrayBlock(batchSize, Optional.ofNullable(dbcv.isNull), dbcv.vector);
                break;
            case DECIMAL:
                /**
                 * PIXELS-196:
                 * Presto reads the unscaled values for decimal type here.
                 * The precision and scale of decimal are automatically processed by Presto.
                 */
                if (vector instanceof DecimalColumnVector)
                {
                    DecimalColumnVector dccv = (DecimalColumnVector) vector;
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(dccv.isNull), dccv.vector);
                }
                else
                {
                    LongDecimalColumnVector ldccv = (LongDecimalColumnVector) vector;
                    block = new Int128ArrayBlock(batchSize, Optional.ofNullable(ldccv.isNull), ldccv.vector);
                }
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            case VARBINARY:
                if (vector instanceof BinaryColumnVector)
                {
                    BinaryColumnVector scv = (BinaryColumnVector) vector;
                    block = new VarcharArrayBlock(batchSize, scv.vector, scv.start, scv.lens, !scv.noNulls, scv.isNull);
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
                block = new ByteArrayBlock(batchSize, Optional.ofNullable(bcv.isNull), bcv.vector);
                break;
            case DATE:
                // PIXELS-94: add date type.
                DateColumnVector dtcv = (DateColumnVector) vector;
                // In pixels and Presto, date is stored as the number of days from UTC 1970-1-1 0:0:0.
                block = new IntArrayBlock(batchSize, Optional.ofNullable(dtcv.isNull), dtcv.dates);
                break;
            case TIME:
                // PIXELS-94: add time type.
                TimeColumnVector tcv = (TimeColumnVector) vector;
                /**
                 * In Presto, LongArrayBlock is used for time type. However, in Pixels,
                 * Time value is stored as int, so here we use TimeArrayBlock, which
                 * accepts int values but provides getLong method same as LongArrayBlock.
                 */
                block = new TimeArrayBlock(batchSize, tcv.times, !tcv.noNulls, tcv.isNull);
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
                block = new LongArrayBlock(batchSize, Optional.ofNullable(tscv.isNull), tscv.times);
                break;
            case VECTOR:
                VectorColumnVector vcv = (VectorColumnVector) vector;
                // builder that simply concatenate all double arrays
                BlockBuilder allDoublesBuilder = DOUBLE.createBlockBuilder(null, batchSize * vcv.dimension);
                int[] offsets = new int[batchSize+1];
                // build a block into which we put a double array
                for (int i = 0 ; i < batchSize; i++)
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
                block = ArrayBlock.fromElementBlock(batchSize, Optional.of(vcv.isNull), offsets, allDoublesBuilder.build());
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
}