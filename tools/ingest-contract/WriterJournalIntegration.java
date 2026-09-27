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
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.MutationTransport;
import io.pixelsdb.pixels.retina.ingest.LocalMutationJournal;
import io.pixelsdb.pixels.trino.write.PixelsMutationWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/** Cross-repository staging integration; not a Trino SQL or commit test. */
public final class WriterJournalIntegration
{
    private static final long TRANSACTION_ID = 100;
    private static final long STATEMENT_ID = 1;
    private static final long FIRST_WRITER_ID = 1;
    private static final long SECOND_WRITER_ID = 2;
    private static final long TABLE_ID = 5;
    private static final long SCHEMA_VERSION = 1;
    private static final int PAYLOAD_FORMAT = 1;
    private static final long MAX_BUFFERED_BYTES = 8192;
    private static final int MAX_STREAMS = 4;

    public static void main(String[] args) throws Exception
    {
        Path directory = Files.createTempDirectory("pixels-writer-journal-");
        List<MutationStreamSeal> receipts = new ArrayList<>();
        try
        {
            try (LocalMutationJournal journal = new LocalMutationJournal(directory, 4096, 1_000_000, 1000))
            {
                MutationTransport transport = new MutationTransport()
                {
                    @Override
                    public CompletableFuture<Void> append(MutationBatch batch)
                    {
                        try
                        {
                            long offset = journal.append(batch);
                            // Simulate retransmission after a lost acceptance response.
                            if (journal.append(batch) != offset) { throw new AssertionError("Retry duplicated batch"); }
                            return CompletableFuture.completedFuture(null);
                        }
                        catch (Exception error) { return failure(error); }
                    }

                    @Override
                    public CompletableFuture<MutationStreamSeal> seal(MutationStreamSeal expected)
                    {
                        try { return CompletableFuture.completedFuture(journal.seal(expected)); }
                        catch (Exception error) { return failure(error); }
                    }
                };
                PixelsMutationWriter first = new PixelsMutationWriter(
                        TRANSACTION_ID, STATEMENT_ID, FIRST_WRITER_ID, TABLE_ID,
                        SCHEMA_VERSION, PAYLOAD_FORMAT, MAX_BUFFERED_BYTES, MAX_STREAMS, transport);
                PixelsMutationWriter second = new PixelsMutationWriter(
                        TRANSACTION_ID, STATEMENT_ID, SECOND_WRITER_ID, TABLE_ID,
                        SCHEMA_VERSION, PAYLOAD_FORMAT, MAX_BUFFERED_BYTES, MAX_STREAMS, transport);
                byte[] equalRows = equalRows(10);
                for (int batch = 0; batch < 50; batch++)
                {
                    PixelsMutationWriter writer = batch % 2 == 0 ? first : second;
                    int shard = (batch / 2) % 2;
                    writer.append(MutationStreamId.Kind.APPEND_ROWS, shard, 10, equalRows).join();
                }
                receipts.addAll(first.finish().join());
                receipts.addAll(second.finish().join());
            }
            long rows = 0;
            int batches = 0;
            try (LocalMutationJournal reopened = new LocalMutationJournal(directory, 4096, 1_000_000, 1000))
            {
                for (MutationStreamSeal receipt : receipts)
                {
                    if (!reopened.getSeal(receipt.getStreamId()).get().equals(receipt))
                    {
                        throw new AssertionError("Writer receipt differs after journal restart");
                    }
                    for (long sequence = 0; sequence < receipt.getBatchCount(); sequence++)
                    {
                        MutationBatch batch = reopened.readSealedBatch(receipt.getStreamId(), sequence);
                        String payload = new String(batch.getPayload(), StandardCharsets.UTF_8);
                        int decodedRows = payload.split("\n").length;
                        if (decodedRows != batch.getRowCount()) { throw new AssertionError("Staging row count differs"); }
                        rows += decodedRows;
                        batches++;
                    }
                }
            }
            if (rows != 500 || batches != 50 || receipts.size() != 4)
            {
                throw new AssertionError("Expected 500 rows, 50 batches, and 4 independently sealed streams");
            }
            System.out.println("WriterJournalIntegration: 500 duplicate-valued rows, 50 retried batches, 4 streams recovered");
        }
        finally
        {
            try (Stream<Path> paths = Files.walk(directory))
            {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new))
                {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static byte[] equalRows(int count)
    {
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < count; i++) { data.append("same-value\n"); }
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static <T> CompletableFuture<T> failure(Throwable error)
    {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }
}
