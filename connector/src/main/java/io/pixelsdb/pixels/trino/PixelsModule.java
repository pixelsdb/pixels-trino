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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class PixelsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(PixelsConnector.class).in(Scopes.SINGLETON);
        binder.bind(PixelsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PixelsClient.class).in(Scopes.SINGLETON);
        binder.bind(PixelsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PixelsRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PixelsConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(PixelsTable.class));
    }
}
