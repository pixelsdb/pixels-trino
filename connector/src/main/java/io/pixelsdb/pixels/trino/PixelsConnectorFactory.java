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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PixelsConnectorFactory implements ConnectorFactory
{
    private static final Logger log = LoggerFactory.getLogger(PixelsConnectorFactory.class);
    private WorkerCoordinateServer coordinateServer;
    @Override
    public String getName()
    {
        return "pixels";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new PixelsModule(catalogName, context.getTypeManager()));
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();
        if (context.getNodeManager().getCurrentNode().isCoordinator())
        {
            PixelsWorkerCoordinator.init();
        }
        return injector.getInstance(PixelsConnector.class);
    }
}
