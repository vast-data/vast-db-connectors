/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.VastTable;
import ndb.NDB;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class VastPartitionedWriteBuilder
        extends VastWriteBuilder
        implements RequiresDistributionAndOrdering
{
    private VastConfig config;

    public VastPartitionedWriteBuilder(VastClient client, VastTable table)
    {
        super(client, table);
        try {
            config = NDB.getConfig();
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Distribution requiredDistribution()
    {
        return Distributions.clustered(vastTable().partitioning());
    }

    @Override
    public boolean distributionStrictlyRequired()
    {
        return config.getInsertExactPartitioning();
    }

    @Override
    public SortOrder[] requiredOrdering()
    {
        return new SortOrder[0];
    }

    @Override
    public long advisoryPartitionSizeInBytes()
    {
        return config.getInsertPartitionSize();
    }
}
