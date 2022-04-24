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
import io.pixelsdb.pixels.trino.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.pixelsdb.pixels.trino.properties.PixelsSessionProperties;
import io.pixelsdb.pixels.trino.properties.PixelsTableProperties;
import io.trino.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class PixelsModule implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public PixelsModule(String connectorId, TypeManager typeManager) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(PixelsConnector.class).in(Scopes.SINGLETON);
        binder.bind(PixelsConnectorId.class).toInstance(new PixelsConnectorId(connectorId));
        binder.bind(PixelsTypeParser.class).in(Scopes.SINGLETON);
        binder.bind(PixelsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PixelsMetadataProxy.class).in(Scopes.SINGLETON);
        binder.bind(PixelsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PixelsPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PixelsRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(PixelsSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PixelsTableProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PixelsTrinoConfig.class);
    }
}
