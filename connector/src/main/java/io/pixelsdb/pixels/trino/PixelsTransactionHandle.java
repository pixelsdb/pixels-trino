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
import com.google.common.util.concurrent.AtomicDouble;
import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class PixelsTransactionHandle implements ConnectorTransactionHandle
{
    /**
     * transId is the transaction id of the query, which is a single-statement read-only transaction.
     */
    private final long transId;
    /**
     * The timestamp that is used to get a read snapshot of the query.
     */
    private long timestamp;
    /**
     * Whether the transaction is read only.
     */
    private final boolean readOnly;
    /**
     * The type of executor to execute this query.
     */
    private ExecutorType executorType;
    /**
     * The accumulative data size in bytes scanned by this transaction (query).
     * This field is not serialized and is only used to calculate the scan size in the coordinator.
     */
    private final AtomicDouble scanBytes;
    /**
     * The cf cost spent on executing this transaction (query) if the query is executed in the cloud.
     */
    private final AtomicDouble cfCostCents;

    /**
     * Create a transaction handle.
     * @param transId the transaction id of the query, which is a single-statement read-only transaction.
     * @param timestamp the timestamp of a transaction.
     * @param readOnly true if the transaction is read only.
     * @param executorType the type of executor to execute this query.
     */
    @JsonCreator
    public PixelsTransactionHandle(@JsonProperty("transId") long transId,
                                   @JsonProperty("timestamp") long timestamp,
                                   @JsonProperty("readOnly") boolean readOnly,
                                   @JsonProperty("executorType") ExecutorType executorType)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        this.readOnly = readOnly;
        this.executorType = executorType;
        this.scanBytes = new AtomicDouble(0);
        this.cfCostCents = new AtomicDouble(0);
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
    public boolean isReadOnly()
    {
        return this.readOnly;
    }

    @JsonProperty
    public ExecutorType getExecutorType()
    {
        return this.executorType;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public void setExecutorType(ExecutorType executorType)
    {
        this.executorType = executorType;
    }

    public void addScanBytes(double columnSize)
    {
        this.scanBytes.addAndGet(columnSize);
    }

    public double getScanBytes()
    {
        return this.scanBytes.get();
    }

    public void addCFCostCents(double costCents)
    {
        this.cfCostCents.addAndGet(costCents);
    }

    public double getCFCostCents()
    {
        return this.cfCostCents.get();
    }
}
