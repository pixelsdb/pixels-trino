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
import io.pixelsdb.pixels.optimizer.queue.QueryQueues;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class PixelsTransactionHandle implements ConnectorTransactionHandle
{
    public static final PixelsTransactionHandle Default = new PixelsTransactionHandle(
            -1, -1, QueryQueues.ExecutorType.None);

    /**
     * transId is also the query id in Pixels, as each query is a single-statement read-only transaction.
     */
    private final long transId;
    /**
     * The timestamp that is used to get a read snapshot of the query.
     */
    private final long timestamp;
    /**
     * The type of executor to execute this query.
     */
    private final QueryQueues.ExecutorType executorType;

    /**
     * Create a transaction handle.
     * @param transId is also the query id in Pixels, as a query is a single-statement read-only transaction.
     * @param timestamp the timestamp of a transaction.
     * @param executorType the type of executor to execute this query.
     */
    @JsonCreator
    public PixelsTransactionHandle(@JsonProperty("transId") long transId,
                                   @JsonProperty("timestamp") long timestamp,
                                   @JsonProperty("executorType")QueryQueues.ExecutorType executorType)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        this.executorType = executorType;
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

    @JsonProperty
    public QueryQueues.ExecutorType getExecutorType()
    {
        return this.executorType;
    }
}
