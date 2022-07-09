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
package io.pixelsdb.pixels.trino.impl;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.*;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.core.exception.PixelsReaderException;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.stats.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * PIXELS-208:
 * This was originally the io.pixelsdb.pixels.core.predicate.TupleDomainPixelsPredicate in pixels-core.
 */

/**
 * Predicate implementation mainly for Presto.
 *
 * @author guodong
 * @author hank
 */
public class PixelsTupleDomainPredicate<C> implements PixelsPredicate
{
    private final TupleDomain<C> predicate;
    private final List<ColumnReference<C>> columnReferences;

    public PixelsTupleDomainPredicate(TupleDomain<C> predicate, List<ColumnReference<C>> columnReferences)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "column references is null"));
    }

    /**
     * Check if the predicate matches the column statistics.
     * Note that on the same column, onlyNull (e.g. 'is null') predicate will match hasNull statistics
     * and vice versa.
     *
     * TODO: pay attention to the correctness of this method.
     *
     * @param numberOfRows            number of rows in the corresponding horizontal data unit
     *                                (pixel, row group, file, etc.) where the statistics come from.
     * @param statisticsByColumnIndex statistics map. key: the index in the file schema of the column to read,
     *                                value: the column statistic of the column to read
     */
    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex)
    {
        /**
         * PIXELS-103:
         * We firstly check if this predicate matches all column statistics.
         * Because according to the implementation of TupleDomain in Presto-0.192,
         * even if matchesAll(), i.e., TupleDomain.isAll() returns true, the domains
         * in the predicate can be empty, and thus we have no way to call
         * domainMatches and turn true.
         */
        if (this.matchesAll())
        {
            return true;
        }
        if (this.matchesNone())
        {
            return false;
        }

        Optional<Map<C, Domain>> optionalDomains = predicate.getDomains();
        if (!optionalDomains.isPresent())
        {
            // predicate is none, so skip the reading content
            return false;
        }
        Map<C, Domain> domains = optionalDomains.get();

        /**
         * PIXELS-103:
         * bugs fixed:
         * 1. return true if there is a match (origin: return false if there is a mismatch).
         * 2. finally return false by default (origin: turn true).
         */
        for (ColumnReference<C> columnReference : columnReferences)
        {
            Domain predicateDomain = domains.get(columnReference.getColumn());
            if (predicateDomain == null)
            {
                // no predicate on this column, so continue
                continue;
            }
            ColumnStats columnStats = statisticsByColumnIndex.get(columnReference.getPhysicalOrdinal());
            if (columnStats == null)
            {
                // no column statistics, so continue
                continue;
            }

            if (domainMatches(columnReference, predicateDomain, numberOfRows, columnStats))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Added in PIXELS-103.
     * This method relies on TupleDomain.isNone() in presto spi,
     * which is mysterious.
     * TODO: pay attention to the correctness of this method.
     * @return true if this predicate will never match any values.
     */
    @Override
    public boolean matchesNone()
    {
        return predicate.isNone();
    }

    /**
     * Added in PIXELS-103.
     * This method relies on TupleDomain.isNone() in presto spi,
     * which is mysterious.
     * TODO: pay attention to the correctness of this method.
     * @return true if this predicate will match any values.
     */
    @Override
    public boolean matchesAll()
    {
        return predicate.isAll();
    }

    /**
     * With query's predicate domain and statistics on the given column, get column domain from the
     * statistics and check if it matches the predicate domain.
     * Note that on the same column, onlyNull (e.g. 'is null') predicate will match hasNull statistics
     * and vice versa.
     * @param columnReference the given column.
     * @param predicateDomain the predicate domain on the column.
     * @param numberOfRows the total number of rows in this horizontal unit (file, row group, or pixel).
     * @param columnStats the statistics on the column.
     * @return
     */
    private boolean domainMatches(ColumnReference<C> columnReference, Domain predicateDomain,
                                   long numberOfRows, ColumnStats columnStats)
    {
        Domain columnDomain = getDomain(columnReference.getType(), numberOfRows, columnStats);
        if (!columnDomain.overlaps(predicateDomain))
        {
            /**
             * PIXELS-103:
             * Even if column domain and predicate domain do not overlap,
             * they can match if either of them is onlyNull while the other
             * one is nullAllowed.
             */
            if (predicateDomain.isNullAllowed() && columnDomain.isNullAllowed())
            {
                return true;
            }
            return false;
        }

        /**
         * PIXELS-103:
         * 1. No need to process discrete values. Discrete values and ranges will not coexist. However either
         * of them has been considered in the Domain.overlaps() method above.
         *
         * 2. Predicate domain and column domain will always be compatible, i.e. their values are all of the
         * EquatableValue or SortedRangeSet type.
         *
         * Reference: the implementation of io.trino.spi.predicate.ValueSet.
         * of and rangesOf method will create the domain values according to the data type.
         * If predicate domain and the column domain are from the same column, they should have the
         * same type of values.
         */
        // Optional<Collection<Object>> discreteValues = getDiscreteValues(predicateDomain.getValues());
        // if (!discreteValues.isPresent())
        // {
        //     return true;
        // }

        return true;
    }

    /**
     * Get all the discrete values, including single values from the ordered ranges
     * and the unordered discrete values.
     * @param valueSet
     * @return
     */
    private Optional<Collection<Object>> getDiscreteValues(ValueSet valueSet)
    {
        return valueSet.getValuesProcessor().transform(
                ranges -> {
                    ImmutableList.Builder<Object> discreteValues = ImmutableList.builder();
                    for (Range range : ranges.getOrderedRanges())
                    {
                        if (!range.isSingleValue())
                        {
                            return Optional.empty();
                        }
                        discreteValues.add(range.getSingleValue());
                    }
                    return Optional.of(discreteValues.build());
                },
                discreteValues -> Optional.of(discreteValues.getValues()),
                allOrNone -> allOrNone.isAll() ? Optional.empty() : Optional.of(ImmutableList.of()));
    }

    /**
     * Get domain object from column statistics.
     * @param type the type of this column.
     * @param rowCount the number of rows in the corresponding horizontal data unit
     *                 (pixel, row group, file, etc.).
     * @param columnStats the statistics of this column in the horizontal data unit.
     * @return
     */
    private Domain getDomain(Type type, long rowCount, ColumnStats columnStats)
    {
        if (rowCount == 0)
        {
            return Domain.none(type);
        }

        if (columnStats == null)
        {
            // PIXELS-103: we have avoided columnStat == null in upper layers of call stack.
            // return Domain.all(type);
            throw new PixelsReaderException("column statistic is null");
        }

        if (columnStats.getNumberOfValues() == 0)
        {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = columnStats.getNumberOfValues() != rowCount || columnStats.hasNull();

        if (type.getJavaType() == boolean.class)
        {
            BooleanColumnStats booleanColumnStats = (BooleanColumnStats) columnStats;
            boolean hasTrueValues = booleanColumnStats.getTrueCount() != 0;
            boolean hasFalseValues = booleanColumnStats.getFalseCount() != 0;
            if (hasTrueValues && hasFalseValues)
            {
                return Domain.all(BOOLEAN);
            }
            if (hasTrueValues)
            {
                return Domain.create(ValueSet.of(BOOLEAN, true), hasNullValue);
            }
            if (hasFalseValues)
            {
                return Domain.create(ValueSet.of(BOOLEAN, false), hasNullValue);
            }
        }
        else if (type instanceof CharType || type instanceof VarcharType)
        {
            return createDomain(type, hasNullValue, (StringColumnStats) columnStats);
        }
        else if (type instanceof TimestampType)
        {
            return createDomain(type, hasNullValue, (TimestampColumnStats) columnStats);
        }
        else if (type instanceof DateType)
        {
            /**
             * PIXELS-103: add Date type predicate.
             */
            return createDomain(type, hasNullValue, (DateColumnStats) columnStats);
        }
        else if (type instanceof  TimeType)
        {
            /**
             * PIXELS-103: add Time type predicate.
             */
            return createDomain(type, hasNullValue, (TimeColumnStats) columnStats);
        }
        else if (type.getJavaType() == Int128.class)
        {
            /**
             * PIXELS-203:
             * In Trino, long decimal with 38 digit precision is backed by Int128.
             * In Pixels, we use Integer128ColumnStats for long decimal columns.
             */
            Integer128ColumnStats int128ColumnStats = (Integer128ColumnStats) columnStats;
            return createDomain(type, hasNullValue, int128ColumnStats,
                    int128 -> Int128.valueOf(int128.getHigh(), int128.getLow()));
        }
        else if (type.getJavaType() == long.class)
        {
            /**
             * PIXELS-208:
             * Besides integer types, decimal type also goes here as decimal in Presto and Trino
             * is backed by long. In Pixels, we also use IntegerColumnStats for decimal
             * columns. If needed in other places, integer statistics can be manually converted
             * to double using the precision and scale from the schema in the file footer.
             */
            return createDomain(type, hasNullValue, (IntegerColumnStats) columnStats);
        }
        else if (type.getJavaType() == double.class)
        {
            return createDomain(type, hasNullValue, (DoubleColumnStats) columnStats);
        }
        /**
         * PIXELS-103:
         * Type unmatched, we should through an exception here instead of returning
         * a domain that can match any predicate.
         */
        // return Domain.create(ValueSet.all(type), hasNullValue);
        throw new PixelsReaderException("unsupported type, display name=" + type.getDisplayName() +
                ", java type=" + type.getJavaType().getName());
    }

    private <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue,
                                                          RangeStats<T> rangeStats)
    {
        // PIXELS-103: what's the purpose of this if-branch?
        //if (type instanceof VarcharType || type instanceof CharType)
        //{
        //    return createDomain(type, hasNullValue, rangeStats, value -> value);
        //}
        // return createDomain(type, hasNullValue, rangeStats, value -> value);
        // PIXELS-103: avoid additional function call.
        T min = rangeStats.getMinimum();
        T max = rangeStats.getMaximum();

        if (min != null && max != null)
        {
            return Domain.create(
                    ValueSet.ofRanges(
                            Range.range(type, min, true, max, true)),
                    hasNullValue);
        }
        if (max != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, max)), hasNullValue);
        }
        if (min != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, min)), hasNullValue);
        }

        /**
         * Comments added in PIXELS-103:
         * If no min nor max is defined, we create a column domain to accepted any predicate.
         */
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private <F, T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue,
                                                             RangeStats<F> rangeStats,
                                                             Function<F, T> function)
    {
        F min = rangeStats.getMinimum();
        F max = rangeStats.getMaximum();

        if (min != null && max != null)
        {
            return Domain.create(
                    ValueSet.ofRanges(
                            Range.range(type, function.apply(min), true, function.apply(max), true)),
                    hasNullValue);
        }
        if (max != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        /**
         * The index of this column in the file schema (the physical column order).
         */
        private final int physicalOrdinal;
        private final Type type;

        /**
         * Create a column reference used by the predicate.
         * @param column the column
         * @param physicalOrdinal the column's ordinal (index) in the file schema (the physical column order)
         * @param type the data type of the column
         */
        public ColumnReference(C column, int physicalOrdinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(physicalOrdinal >= 0, "physicalOrdinal is negative");
            this.physicalOrdinal = physicalOrdinal;
            this.type = requireNonNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getPhysicalOrdinal()
        {
            return physicalOrdinal;
        }

        public Type getType()
        {
            return type;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("PixelsTupleDomainPredicate{isNone=")
                .append(predicate.isNone() + ", isAll=")
                .append(predicate.isAll() + ", ")
                .append("columnPredicates=[");
        boolean deleteLast = false;
        for (TupleDomain.ColumnDomain cd : predicate.getColumnDomains().get())
        {
            builder.append("{column=" + cd.getColumn() + ",nullable=" + cd.getDomain().isNullAllowed())
                    .append(",isSingleValue=" + cd.getDomain().isNullableSingleValue())
                    .append(",isNone=" + cd.getDomain().getValues().isNone())
                    .append(",isAll=" + cd.getDomain().getValues().isAll())
                    .append("},");
            deleteLast = true;
        }
        if (deleteLast)
        {
            builder.deleteCharAt(builder.length()-1);
            deleteLast = false;
        }
        builder.append("], columnReferences=[");
        for (ColumnReference cr : columnReferences)
        {
            builder.append("{columnName=" + cr.getColumn())
                    .append(", columnType=" + cr.getType().getDisplayName())
                    .append("},");
            deleteLast = true;
        }
        if (deleteLast)
        {
            builder.deleteCharAt(builder.length()-1);
        }
        builder.append("]}");
        return builder.toString();
    }
}
