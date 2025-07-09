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

import io.pixelsdb.pixels.trino.split.PixelsFileSplit;
import io.pixelsdb.pixels.trino.split.PixelsSplit;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author hank
 */
public class PixelsSplitSource implements ConnectorSplitSource
{
    private final List<PixelsSplit> splits;
    private int offset;

    public PixelsSplitSource(List<PixelsSplit> splits)
    {
        requireNonNull(splits, "splits is null");
        this.splits = splits;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = new ArrayList<>(size);
        for (int i = offset; i < offset+size; ++i)
        {
            results.add(splits.get(i));
        }
        offset += size;

        return completedFuture(new ConnectorSplitBatch(results, isFinished()));
    }

    @Override
    public void close()
    {

    }

    /**
     * Returns whether anymore {@link PixelsSplit} may be produced.
     * <p>
     * This method should only be called when there has been no invocation of getNextBatch,
     * or result Future of previous getNextBatch is done.
     * Calling this method at other time is not useful because the contract of such an invocation
     * will be inherently racy.
     */
    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }
}
