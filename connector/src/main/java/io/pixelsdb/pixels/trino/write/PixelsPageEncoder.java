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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino.write;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.ingest.IngestRows;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOS_PER_MILLIS;

/** Converts Trino native Blocks to existing Pixels canonical scalar bytes, never SQL strings. */
public final class PixelsPageEncoder {
    private static final int INT128_LONG_WORD_COUNT = 2;

    private final TableSpec table;
    private final int[] inputChannels;
    private final Type[] inputTypes;
    private final TypeDescription[] storageTypes;
    private final int inputCount;

    public PixelsPageEncoder(TableSpec table, List<PixelsColumnHandle> inputs, TypeManager types)
            throws IOException {
        this.table = table;
        inputCount = inputs.size();
        inputChannels = new int[table.getColumnsCount()];
        Arrays.fill(inputChannels, -1);
        inputTypes = new Type[table.getColumnsCount()];
        storageTypes = new TypeDescription[table.getColumnsCount()];
        Map<String, Integer> ordinals = new HashMap<>();
        for (int i = 0; i < table.getColumnsCount(); i++) {
            storageTypes[i] = IngestRows.supported(table.getColumns(i).getType());
            ordinals.put(table.getColumns(i).getName(), i);
            inputTypes[i] = types.fromSqlType(table.getColumns(i).getType());
        }
        for (int channel = 0; channel < inputs.size(); channel++) {
            PixelsColumnHandle column = inputs.get(channel);
            Integer ordinal = ordinals.get(column.getColumnName());
            if (ordinal == null
                    || inputChannels[ordinal] >= 0
                    || !inputTypes[ordinal].equals(column.getColumnType())) {
                throw new IOException(
                        "INSERT input does not match pinned schema: " + column.getColumnName());
            }
            inputChannels[ordinal] = channel;
        }
    }

    public byte[][] encodeRow(Page page, int position) throws IOException {
        if (page.getChannelCount() != inputCount) {
            throw new IOException("INSERT input channel count changed");
        }
        byte[][] result = new byte[inputChannels.length][];
        for (int column = 0; column < inputChannels.length; column++) {
            int channel = inputChannels[column];
            if (channel < 0 || page.getBlock(channel).isNull(position)) {
                continue;
            }
            result[column] =
                    encode(
                            inputTypes[column],
                            storageTypes[column],
                            page.getBlock(channel),
                            position);
        }
        IngestRows.validate(table, result);
        return result;
    }

    static byte[] encode(Type type, TypeDescription storage, Block block, int position)
            throws IOException {
        switch (storage.getCategory()) {
            case BOOLEAN:
                return new byte[] {(byte) (type.getBoolean(block, position) ? 1 : 0)};
            case BYTE:
                {
                    long value = type.getLong(block, position);
                    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                        throw new IOException("TINYINT overflow");
                    }
                    return new byte[] {(byte) value};
                }
            case SHORT:
                {
                    long value = type.getLong(block, position);
                    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                        throw new IOException("SMALLINT overflow");
                    }
                    return ByteBuffer.allocate(Short.BYTES).putShort((short) value).array();
                }
            case INT:
            case DATE:
                return ByteBuffer.allocate(Integer.BYTES)
                        .putInt(Math.toIntExact(type.getLong(block, position)))
                        .array();
            case LONG:
            case TIMESTAMP:
                return ByteBuffer.allocate(Long.BYTES).putLong(type.getLong(block, position)).array();
            case TIME:
                {
                    long picos = type.getLong(block, position);
                    if (picos % PICOS_PER_MILLIS != 0) {
                        throw new IOException(
                                "TIME cannot be represented losslessly in Pixels milliseconds");
                    }
                    return ByteBuffer.allocate(Integer.BYTES)
                            .putInt(Math.toIntExact(picos / PICOS_PER_MILLIS))
                            .array();
                }
            case FLOAT:
                return ByteBuffer.allocate(Float.BYTES).putInt((int) type.getLong(block, position)).array();
            case DOUBLE:
                return ByteBuffer.allocate(Double.BYTES).putDouble(type.getDouble(block, position)).array();
            case DECIMAL:
                {
                    DecimalType decimal = (DecimalType) type;
                    if (decimal.isShort()) {
                        return ByteBuffer.allocate(Long.BYTES)
                                .putLong(type.getLong(block, position))
                                .array();
                    }
                    Int128 value = (Int128) type.getObject(block, position);
                    return ByteBuffer.allocate(
                                    Math.multiplyExact(Long.BYTES, INT128_LONG_WORD_COUNT))
                            .putLong(value.getHigh())
                            .putLong(value.getLow())
                            .array();
                }
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            case VARBINARY:
                return type.getSlice(block, position).getBytes();
            default:
                throw new IOException("Unsupported transactional INSERT type: " + storage);
        }
    }
}
