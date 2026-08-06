/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.TableLayout;
import com.vastdata.client.MockClientHelper;
import com.vastdata.client.MockClientHelper.MockTableHelper;
import com.vastdata.client.VastClient;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

public class TestDropPartitions
        extends QueryRunnerTest
{
    VastClient vastClient;
    VastTrinoTransactionHandleManager transactionHandleManager;

    protected VastModule getVastModule()
    {
        vastClient = mock(VastClient.class);
        transactionHandleManager = mock(VastTrinoTransactionHandleManager.class);

        VastModule vastModule = VastModule
                .builder(false)
                .withVastClient(vastClient)
                .withTransactionManager(transactionHandleManager)
                .build();
        return vastModule;
    }

    @BeforeEach
    public void setUp()
    {
        reset(vastClient, transactionHandleManager);
    }

    @Test
    public void testInnerSelectNotSupported()
    {
        String schema = "b/s";
        String table = "t";
        String pitTable = "t$partitions";
        String pitTablePath = "vast.\"%s\".\"%s\"".formatted(schema, pitTable);
        String tablePathNonAcidAllowed = "vast.\"%s\".\"%s vast.allow_non_acid\"".formatted(schema, pitTable);

        MockClientHelper mockClientHelper = MockClientHelper.forClient(vastClient);
        MockTableHelper mockTableHelper = mockClientHelper.registerTable(schema, table);

        mockTableHelper.withTableLayout(
                new TableLayout(new Schema(List.of(new Field("c", FieldType.nullable(new Int(32, true)), List.of()))),
                        List.of(),
                        List.of(new PartitionColumnMetadata("c", "integer", "c", "integer", "identity", null))));

        MockTxManagerHelper mockTxManagerHelper = MockTxManagerHelper.forTxManager(transactionHandleManager);
        mockTxManagerHelper.registerTx(42);

        assertQueryFails("DELETE FROM %s WHERE c IN (SELECT c FROM %s WHERE c = 23)".formatted(tablePathNonAcidAllowed,
                pitTablePath), "Row-level modifications are not supported on PIT table.*");
    }
}
