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
package io.pixelsdb.pixels.trino.write;

import static io.trino.spi.StandardErrorCode.*;

import com.google.inject.Inject;

import io.pixelsdb.pixels.ingest.IngestProto.AllocateWriterRequest;
import io.pixelsdb.pixels.ingest.IngestProto.WriterAssignment;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.UUID;

/** Worker-local sinks over the shared ingestion transport. */
public final class PixelsPageSinkProvider implements ConnectorPageSinkProvider {
    private final PixelsIngestTransactions ingest;
    private final TypeManager types;

    @Inject
    public PixelsPageSinkProvider(PixelsIngestTransactions ingest, TypeManager types) {
        this.ingest = ingest;
        this.types = types;
    }

    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle output,
            ConnectorPageSinkId id) {
        throw new TrinoException(NOT_SUPPORTED, "Pixels CTAS is not enabled");
    }

    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorInsertTableHandle input,
            ConnectorPageSinkId id) {
        try {
            PixelsInsertTableHandle handle = (PixelsInsertTableHandle) input;
            return new PixelsInsertPageSink(
                    handle,
                    () -> allocateWriter(handle, id),
                    types,
                    ingest.client().transport(handle.decodeTable()),
                    ingest.options());
        } catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot create Retina INSERT sink", e);
        }
    }

    private long allocateWriter(PixelsInsertTableHandle handle, ConnectorPageSinkId id) {
        try {
            // Trino 466 derives PageSinkId from TaskId; parallel writer operators
            // in one task need separate ingestion streams and sequence spaces.
            AllocateWriterRequest request =
                    AllocateWriterRequest.newBuilder()
                            .setTransactionId(handle.getTransactionId())
                            .setTaskId(id.getId())
                            .setRequestId(UUID.randomUUID().toString())
                            .setStatementId(handle.getStatementId())
                            .build();
            WriterAssignment writer = ingest.client().coordinator().allocateWriter(request);
            if (writer.getWriterId() <= 0
                    || writer.getTaskId() != request.getTaskId()
                    || writer.getStatementId() != request.getStatementId()
                    || !writer.getRequestId().equals(request.getRequestId())) {
                throw new IOException("Invalid ingestion writer assignment");
            }
            return writer.getWriterId();
        } catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot allocate Retina INSERT writer", e);
        }
    }
}
