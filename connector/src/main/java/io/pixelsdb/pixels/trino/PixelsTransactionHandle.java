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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class PixelsTransactionHandle implements ConnectorTransactionHandle
{
    public static final PixelsTransactionHandle Default = new PixelsTransactionHandle(-1, -1);

    /**
     * transId is also the query id, as each query is a single-statement read-only transaction.
     */
    private final long transId;
    /**
     * The timestamp that is used to get a read snapshot of the query.
     */
    private final long timestamp;

    /**
     * Create a transaction handle.
     * @param transId is also the queryId as a query is a single-statement read-only transaction.
     * @param timestamp the timestamp of a transaction.
     */
    @JsonCreator
    public PixelsTransactionHandle(@JsonProperty("transId") long transId,
                                   @JsonProperty("timestamp") long timestamp)
    {
        this.transId = transId;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public long getTransId()
    {
        return this.transId;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return this.timestamp;
    }
}
