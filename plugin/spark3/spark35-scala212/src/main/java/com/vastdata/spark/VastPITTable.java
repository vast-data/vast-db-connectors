/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class VastPITTable
        extends VastTable
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPITTable.class);

    public VastPITTable(VastCatalogUtils vastCatalogUtils, String schemaName,
            String tableName, String handleID, StructType schema,
            Transform[] partitioning, Supplier<VastClient> clientSupplier,
            Optional<RuntimeException> notSafeToWrite,
            Map<String, String> additionalProperties)
    {
        super(vastCatalogUtils, schemaName, tableName, handleID, schema,
                partitioning, clientSupplier, false, notSafeToWrite,
                additionalProperties);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        LOG.debug("Creating VastPITScanBuilder for PIT table: {}", this);
        return new VastPITScanBuilder(this, vastCatalogUtils);
    }
}
