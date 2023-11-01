/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastVersion;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class VastSystemTable
        implements SystemTable
{
    public static final SchemaTableName NAME = new SchemaTableName("system_schema", "release");

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return new ConnectorTableMetadata(NAME, List.of(
                new ColumnMetadata("version", VARCHAR),
                new ColumnMetadata("hash", VARCHAR)));
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return InMemoryRecordSet.builder(getTableMetadata())
                .addRow(VastVersion.SYS_VERSION, VastVersion.HASH)
                .build()
                .cursor();
    }
}
