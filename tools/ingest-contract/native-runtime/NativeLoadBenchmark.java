package io.pixelsdb.pixels.daemon.transaction.ingest;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.cli.load.LoadedInfo;
import io.pixelsdb.pixels.cli.load.Parameters;
import io.pixelsdb.pixels.cli.load.SimplePixelsConsumer;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Native CLI consumer baseline for the verified two-column TPC INSERT dataset. */
public final class NativeLoadBenchmark {
    private static final int PIXEL_STRIDE = 10_000;
    private static final int SINGLE_ROW_GROUP_BYTES = Integer.MAX_VALUE;

    private static PixelsReader open(Path path) throws Exception {
        String uri = path.toUri().toString();
        return PixelsReaderImpl.newBuilder()
                .setStorage(StorageFactory.Instance().getStorage(uri))
                .setPath(uri).setPixelsFooterCache(new PixelsFooterCache()).build();
    }

    private static List<Path> files(Path directory) throws Exception {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
    }

    /** Preparation is excluded from the CLI load timer. */
    private static long export(Path source, Path target, String prefix) throws Exception {
        long rows = 0;
        for (Path file : files(source)) {
            if (!file.getFileName().toString().endsWith(".pxl")) {
                continue;
            }
            Path csv = target.resolve(file.getFileName() + ".csv");
            long fileRows = 0;
            try (PixelsReader reader = open(file);
                    PixelsRecordReader records = reader.read(new PixelsReaderOption()
                            .includeCols(new String[]{"id", "label"}));
                    BufferedWriter output = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
                VectorizedRowBatch batch;
                while ((batch = records.readBatch()) != null && batch.size > 0) {
                    LongColumnVector ids = (LongColumnVector) batch.cols[0];
                    BinaryColumnVector labels = (BinaryColumnVector) batch.cols[1];
                    for (int row = 0; row < batch.size; row++) {
                        int idPosition = ids.isRepeating() ? 0 : row;
                        int labelPosition = labels.isRepeating() ? 0 : row;
                        if ((!ids.noNulls && ids.isNull[idPosition])
                                || (!labels.noNulls && labels.isNull[labelPosition])) {
                            throw new IllegalStateException("Benchmark projection contains NULL");
                        }
                        String label = new String(labels.vector[labelPosition],
                                labels.start[labelPosition], labels.lens[labelPosition],
                                StandardCharsets.UTF_8);
                        if (!label.startsWith(prefix + ":")) {
                            continue;
                        }
                        if (label.indexOf('|') >= 0 || label.indexOf('\n') >= 0
                                || label.indexOf('\r') >= 0) {
                            throw new IllegalStateException("Value is not representable by CLI delimited input");
                        }
                        output.write(Long.toString(ids.vector[idPosition]));
                        output.write('|');
                        output.write(label);
                        output.newLine();
                        fileRows++;
                    }
                }
            }
            if (fileRows == 0) {
                Files.delete(csv);
            }
            rows += fileRows;
        }
        return rows;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("sourcePixelsDir freshWorkDir tpch|tpcds threads fileRows");
        }
        Path source = Paths.get(args[0]);
        Path work = Paths.get(args[1]);
        String dataset = args[2];
        int threads = Integer.parseInt(args[3]);
        int fileRows = Integer.parseInt(args[4]);
        if (Files.exists(work) || threads <= 0 || fileRows <= 0
                || !(dataset.equals("tpch") || dataset.equals("tpcds"))) {
            throw new IllegalArgumentException("Use a fresh work directory and positive load limits");
        }
        Files.createDirectories(work);
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("retina.enable", "false");
        config.addProperty("retina.ingest.enabled", "false");
        config.addProperty("pixel.stride", Integer.toString(PIXEL_STRIDE));
        config.addProperty("row.group.size", Integer.toString(SINGLE_ROW_GROUP_BYTES));
        Path input = Files.createDirectory(work.resolve("input"));
        long expectedRows = export(source, input, dataset);
        if (expectedRows == 0) {
            throw new IllegalStateException("No matching source rows");
        }
        System.out.println("NATIVE_LOAD_INPUT_READY dataset=" + dataset + " rows=" + expectedRows);

        Path output = Files.createDirectory(work.resolve("output"));
        try (SqlIngestFixture.Catalog catalog = new SqlIngestFixture.Catalog(output)) {
            Server metadataServer = ServerBuilder.forPort(0).addService(catalog).build().start();
            ExecutorService consumers = Executors.newFixedThreadPool(threads);
            try {
                config.addProperty("metadata.server.host", "127.0.0.1");
                config.addProperty("metadata.server.port", Integer.toString(metadataServer.getPort()));
                MetadataService metadata = MetadataService.Instance();
                Parameters parameters = new Parameters("s", "t", fileRows, "|",
                        EncodingLevel.from(Integer.parseInt(config.getProperty("retina.buffer.flush.encodingLevel"))),
                        Boolean.parseBoolean(config.getProperty("retina.buffer.flush.nullsPadding")),
                        metadata, 1L, 1L);
                if (!parameters.initExtra()) {
                    throw new IllegalStateException("Cannot initialize CLI load layout");
                }
                BlockingQueue<String> queue = new LinkedBlockingQueue<>();
                for (Path file : files(input)) {
                    queue.add(file.toUri().toString());
                }
                ConcurrentLinkedQueue<LoadedInfo> loaded = new ConcurrentLinkedQueue<>();
                List<Future<?>> tasks = new ArrayList<>();
                long start = System.nanoTime();
                for (int thread = 0; thread < threads; thread++) {
                    tasks.add(consumers.submit(new SimplePixelsConsumer(queue, parameters, loaded)));
                }
                for (Future<?> task : tasks) {
                    task.get();
                }
                for (LoadedInfo info : loaded) {
                    if (!metadata.updateFile(info.loadedFile)) {
                        throw new IllegalStateException("CLI file publication failed");
                    }
                }
                long elapsed = System.nanoTime() - start;
                long actualRows = 0;
                long bytes = 0;
                for (Path file : files(output.resolve("ordered"))) {
                    try (PixelsReader reader = open(file)) {
                        actualRows += reader.getNumberOfRows();
                    }
                    bytes += Files.size(file);
                }
                if (actualRows != expectedRows) {
                    throw new AssertionError("CLI output rows differ: " + actualRows + " != " + expectedRows);
                }
                System.out.printf(Locale.ROOT,
                        "NATIVE_LOAD_PASS dataset=%s rows=%d elapsedMs=%d rowsPerSecond=%.2f files=%d bytes=%d threads=%d%n",
                        dataset, actualRows, TimeUnit.NANOSECONDS.toMillis(elapsed),
                        actualRows * 1_000_000_000.0 / elapsed, loaded.size(), bytes, threads);
            } finally {
                consumers.shutdownNow();
                metadataServer.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }
}
