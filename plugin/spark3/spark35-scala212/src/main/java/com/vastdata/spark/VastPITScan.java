/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VastPITScan
        extends VastScan
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPITScan.class);

    public VastPITScan(int scanBuilderID, VastPITTable table, StructType schema,
            Integer limit, List<List<VastPredicate>> predicates)
    {
        super(scanBuilderID, table, schema, limit, predicates);
    }

    @Override
    public VastBatch toBatch()
    {
        LOG.info("Building VastPITBatch with schema: {}", schema);
        return new VastPITBatch((VastPITTable) table, readSchema(),
                pushDownPredicates);
    }
}
