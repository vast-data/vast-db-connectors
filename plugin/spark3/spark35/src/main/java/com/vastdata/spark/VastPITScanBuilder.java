/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.VastCatalogUtils;

public class VastPITScanBuilder
        extends VastScanBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPITScanBuilder.class);

    public VastPITScanBuilder(VastPITTable table,
            VastCatalogUtils vastCatalogUtils)
    {
        super(table, vastCatalogUtils);
    }

    @Override
    public Scan build()
    {
        LOG.info("Building VastPITScan with schema: {}", schema);
        return new VastPITScan(scanBuilderID, (VastPITTable) table, schema,
                limit, pushedDownPredicates);
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        LOG.info("Pruning VastPITScan with schema: {}", requiredSchema);
        super.pruneColumns(requiredSchema);
    }
}
