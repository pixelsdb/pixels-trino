/*
 * Copyright 2024 PixelsDB.
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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;

/**
 * @author huasiy
 * Worker coordiantor will run on specified port after init.
 */
public class PixelsWorkerCoordinator
{
    private static WorkerCoordinateServer instance;
    private static Thread thread = null;

    private PixelsWorkerCoordinator() {}

    public static void init()
    {
        if (instance == null)
        {
            int port = Integer.parseInt(ConfigFactory.Instance().getProperty("worker.coordinate.server.port"));
            instance = new WorkerCoordinateServer(port);
            thread = new Thread(instance);
            thread.start();
        }
    }
    public static void shutdown()
    {
        thread.interrupt();
    }
}
