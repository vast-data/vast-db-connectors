/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.StringJoiner;

import static java.lang.String.format;

public class VastRowLevelOperationBuilder
        implements RowLevelOperationBuilder
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
        if (info.command().equals(RowLevelOperation.Command.DELETE)) {
            return new RowLevelDelete(vastTable);
        }
        else if (info.command().equals(RowLevelOperation.Command.UPDATE)) {
            return new RowLevelUpdate(vastTable);
        }
        throw new UnsupportedOperationException(format("Unsupported row level operation: %s", info));
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
