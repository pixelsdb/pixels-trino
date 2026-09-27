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
package io.pixelsdb.pixels.trino.write;

import org.junit.jupiter.api.Test;

public class TestPixelsMutationWriter
{
    @Test
    public void identitiesCountsAndKinds() throws Exception
    {
        MutationWriterContract.identitiesCountsAndKinds();
    }

    @Test
    public void finishWaitsForInflightAppend() throws Exception
    {
        MutationWriterContract.finishWaitsForInflightAppend();
    }

    @Test
    public void requestOrder() throws Exception
    {
        MutationWriterContract.requestOrder();
    }

    @Test
    public void boundedAdmissionDoesNotConsumeSequence() throws Exception
    {
        MutationWriterContract.boundedAdmissionDoesNotConsumeSequence();
    }

    @Test
    public void appendFailurePreventsSeal() throws Exception
    {
        MutationWriterContract.appendFailurePreventsSeal();
    }

    @Test
    public void synchronousTransportFailure() throws Exception
    {
        MutationWriterContract.synchronousTransportFailure();
    }

    @Test
    public void incorrectReceiptRejected() throws Exception
    {
        MutationWriterContract.incorrectReceiptRejected();
    }

    @Test
    public void abortStopsQueuedCalls() throws Exception
    {
        MutationWriterContract.abortStopsQueuedCalls();
    }

    @Test
    public void emptyWriterDoesNotOpenStreams() throws Exception
    {
        MutationWriterContract.emptyWriterDoesNotOpenStreams();
    }

    @Test
    public void payloadCopiesAndStreamLimit() throws Exception
    {
        MutationWriterContract.payloadCopiesAndStreamLimit();
    }

    @Test
    public void sealFailurePropagates() throws Exception
    {
        MutationWriterContract.sealFailurePropagates();
    }

    @Test
    public void completionCanFinishReentrantly() throws Exception
    {
        MutationWriterContract.completionCanFinishReentrantly();
    }

    @Test
    public void invalidInputDoesNotOpenStream() throws Exception
    {
        MutationWriterContract.invalidInputDoesNotOpenStream();
    }
}
