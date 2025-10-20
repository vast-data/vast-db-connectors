/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.vastdata.client.VastClient;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;

public class VastWriteBuilder
        implements DeltaWriteBuilder, DeltaWrite
{
    private final VastTable table;
    private final VastClient client;

    public VastWriteBuilder(VastClient client, VastTable table) {
        this.table = table;
        this.client = client;
    }

    @Override
    public DeltaWrite build()
    {
        return this;
    }

    @Override
    public DeltaBatchWrite toBatch()
    {
        return new VastBatchWriter(client, table);
    }
}
