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
package io.pixelsdb.pixels.trino.properties;

import io.trino.spi.session.PropertyMetadata;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

/**
 * Class contains all table properties for the Pixels connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (storage = 'hdfs');
 * </p>
 *
 * @author hank
 */
public class PixelsTableProperties
{
    public static final String STORAGE = "storage";
    public static final String PATHS = "paths";
    public static final String PRIMARY_KEY = "pk";
    public static final String PRIMARY_KEY_SCHEME = "pk_scheme";
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PixelsTableProperties()
    {
        PropertyMetadata<String> s1 = stringProperty(
                STORAGE, "The storage scheme of the table.", "file", false);

        PropertyMetadata<String> s2 = stringProperty(
                PATHS, "The storage paths of the table.", null, false);

        PropertyMetadata<String> s3 = stringProperty(
                PRIMARY_KEY, "The primary key of the table.", null, false);

        PropertyMetadata<String> s4 = stringProperty(
                PRIMARY_KEY_SCHEME, "The primary key index scheme of the table.", "rocksdb", false);
        tableProperties = ImmutableList.of(s1, s2, s3, s4);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
