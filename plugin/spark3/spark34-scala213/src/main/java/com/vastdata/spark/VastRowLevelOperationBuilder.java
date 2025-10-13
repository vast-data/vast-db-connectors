/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.StringJoiner;

public class VastRowLevelOperationBuilder
        implements RowLevelOperationBuilder, RowLevelOperation
{
    private static final Logger LOG = LoggerFactory.getLogger(VastRowLevelOperationBuilder.class);

    private final VastTable vastTable;
    private final RowLevelOperationInfo info;

    public VastRowLevelOperationBuilder(VastTable vastTable, RowLevelOperationInfo info) {
        this.vastTable = Objects.requireNonNull(vastTable);
        this.info = Objects.requireNonNull(info);
    }

    @Override
    public RowLevelOperation build()
    {
        LOG.info("build(): {}", this);
        return this;
    }

    @Override
    public Command command()
    {
        return info.command();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap)
    {
        LOG.info("newScanBuilder(): {}", caseInsensitiveStringMap.asCaseSensitiveMap());
        return vastTable.newScanBuilder(caseInsensitiveStringMap);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo)
    {
        LOG.info("newWriteBuilder(): {}, {}, {}", logicalWriteInfo.queryId(), logicalWriteInfo.schema(), logicalWriteInfo.options().asCaseSensitiveMap());
        return vastTable.newWriteBuilder(logicalWriteInfo);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", VastRowLevelOperationBuilder.class.getSimpleName() + "[", "]")
                .add("vastTable=" + vastTable.getTableMD())
                .add("command=" + info.command())
                .add("options=" + info.options().asCaseSensitiveMap())
                .toString();
    }
}
