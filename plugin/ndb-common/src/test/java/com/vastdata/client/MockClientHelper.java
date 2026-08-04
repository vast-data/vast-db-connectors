/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableList;
import com.vastdata.TableLayout;
import com.vastdata.client.stats.VastStatistics;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class MockClientHelper
{
    private final VastClient client;

    private MockClientHelper(VastClient client)
    {
        this.client = client;
    }

    public static MockClientHelper forClient(VastClient client)
    {
        return new MockClientHelper(client);
    }

    public MockTableHelper registerTable(String schema, String table)
    {
        return new MockTableHelper(client, schema, table);
    }

    public static class MockTableHelper
    {
        private final VastClient client;
        private final String schema;
        private final String table;

        public MockTableHelper(VastClient client, String schema, String table)
        {
            this.client = client;
            this.schema = schema;
            this.table = table;

            registerTableMetadata();
        }

        private void registerTableMetadata()
        {
            VastObjectDetails objectDetails = new VastObjectDetails(table, "", "", 0, 0, 0, false, 0, 0, 0, 0);
            try {
                when(client.schemaExists(any(), eq(schema), anyString())).thenReturn(true);
                when(client.getTableStats(any(), eq(schema), anyString(), anyString())).thenReturn(new VastStatistics(0L, 0L));
                when(client.listAllSchemas(any(), anyInt(), anyString())).thenReturn(ImmutableList.of(schema).stream());
                when(client.getVastTableHandleId(any(), eq(schema), eq(table), anyString())).thenReturn(
                        Optional.of(objectDetails));

                String tableStatsKey = String.format("%s.VastTrinoPlugin.v1.stats", table);
                when(client.s3GetObj(eq(tableStatsKey), eq(schema))).thenReturn(Optional.empty());
            }
            catch (Exception e) {
            }
        }

        private void makePitTable(TableLayout baseLayout)
        {
            MockTableHelper pitHelper = new MockTableHelper(client, schema,
                    String.format("%s%s", table, PIT_NAME_SUFFIX));
            List<Field> fields = baseLayout.getPartitionColumnsMetadata().stream().map(metadata ->
            {
                Field field = baseLayout.getSchema().findField(metadata.getColumnName());
                if (field == null) {
                    ArrowType arrowType;
                    switch (metadata.getColumnType().toLowerCase(Locale.US)) {
                        case "integer":
                            arrowType = new ArrowType.Int(32, true);
                            break;
                        default:
                            throw new RuntimeException("Option in test util not implemented");
                    }
                    field = new Field(metadata.getColumnName(), FieldType.nullable(arrowType), null);
                }
                return field;
            }).collect(Collectors.toList());
            TableLayout pitLayout = TableLayout.regularTable(new Schema(fields));
            pitHelper.withTableLayout(pitLayout);
        }

        public MockTableHelper withTableLayout(TableLayout layout)
        {
            try {
                when(client.fetchTableLayout(any(), eq(schema), eq(table), anyInt(), any(), anyString())).thenReturn(
                        layout);
            }
            catch (Exception e) {
            }

            if (layout.hasPartitionColumns()) {
                makePitTable(layout);
            }

            return this;
        }
    }
}
