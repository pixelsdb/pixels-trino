package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;

public class PixelsWorkerCoordinator {
    private static WorkerCoordinateServer instance;

    private PixelsWorkerCoordinator() {}

    public static void init()
    {
        if (instance == null) {
            int port = Integer.parseInt(ConfigFactory.Instance().getProperty("worker.coordinate.server.port"));
            instance = new WorkerCoordinateServer(port);
            Thread workerThread = new Thread(instance);
            workerThread.start();
        }
    }
}
