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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

/**
 * @author hank
 */
public class PixelsSessionProperties
{
    private static final String ORDERED_PATH_ENABLED = "ordered_path_enabled";
    private static final String COMPACT_PATH_ENABLED = "compact_path_enabled";
    private static final String CLOUD_FUNCTION_ENABLED = "cloud_function_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PixelsSessionProperties()
    {
        PropertyMetadata<Boolean> s1 = booleanProperty(
                ORDERED_PATH_ENABLED,
                "Set true to enable the ordered path for queries.", true, false);

        PropertyMetadata<Boolean> s2 = booleanProperty(
                COMPACT_PATH_ENABLED,
                "Set true to enable the compact path for queries.", true, false);

        PropertyMetadata<Boolean> s3 = booleanProperty(
                CLOUD_FUNCTION_ENABLED,
                "Set true to enable cloud-function workers for query processing,",
                false, false);

        sessionProperties = ImmutableList.of(s1, s2, s3);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean getOrderedPathEnabled(ConnectorSession session)
    {
        return session.getProperty(ORDERED_PATH_ENABLED, Boolean.class);
    }

    public static boolean getCompactPathEnabled(ConnectorSession session)
    {
        return session.getProperty(COMPACT_PATH_ENABLED, Boolean.class);
    }

    public static boolean getCloudFunctionEnabled(ConnectorSession session)
    {
        return session.getProperty(CLOUD_FUNCTION_ENABLED, Boolean.class);
    }
}
