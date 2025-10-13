/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.vastdata.client.VastClient;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class VastWriteBuilder
        implements WriteBuilder, Write
{
    private final VastTable table;
    private final VastClient client;

    public VastWriteBuilder(VastClient client, VastTable table) {
        this.table = table;
        this.client = client;
    }

    @Override
    public Write build()
    {
        return this;
    }

    @Override
    public BatchWrite toBatch()
    {
        return new VastBatchWriter(client, table);
    }
}
