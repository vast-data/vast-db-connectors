/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class VastCatalogTestUtils
        extends VastCatalogUtils
{
    private static final RowColumnSecurityResponse noRowColumnSecurityResponse = new RowColumnSecurityResponse(
            emptyList(), emptySet(), emptySet(), emptyMap());
    private final Map<String, Map<String, RowColumnSecurityResponse>> responses = new HashMap<>();

    public VastCatalogTestUtils(VastConfig config, VastClient vastClient,
            VastTransactionHandleManager<SimpleVastTransaction> transactionsManager)
    {
        super(config, vastClient, transactionsManager);
    }

    public void setRowColumnsSecurityResponse(String schema, String table,
            RowColumnSecurityResponse rowColumnSecurityResponse)
    {
        responses.computeIfAbsent(schema, s -> new HashMap<>()).put(table,
                rowColumnSecurityResponse);
    }

    @Override
    public RowColumnSecurityResponse getRowColumnSecurity(String schema,
            String table, String endUser)
    {
        if (responses.containsKey(schema) && responses.get(schema).containsKey(
                table)) {
            return responses.get(schema).get(table);
        }
        return noRowColumnSecurityResponse;
    }
}
