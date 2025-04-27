/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

final public class VastTableReadOnly extends VastTable {
    private static final Logger LOG = LoggerFactory.getLogger(VastTable.class);
    private final RuntimeException notSafeToWrite;

    public VastTableReadOnly(final String schemaName,
                             final String tableName,
                             final String handleID,
                             final StructType schema,
                             final Supplier<VastClient> clientSupplier,
                             final boolean forImportData,
                             final RuntimeException notSafeToWrite)
    {
        super(schemaName, tableName, handleID, schema, clientSupplier, forImportData);
        this.notSafeToWrite = notSafeToWrite;
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info)
    {
        final RuntimeException error = new RuntimeException(notSafeToWrite.getMessage(), notSafeToWrite);
        LOG.error("Write attempt with an unsafe Spark configuration", error);
        throw error;
    }

}
