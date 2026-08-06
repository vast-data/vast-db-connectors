/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastSplitContext;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

public class GetTableSizeHelper
{
    private GetTableSizeHelper()
    {
    }

    public static long getTableSizeEstimate(VastPageSourceProvider vastPageSourceProvider,
                                            final VastTableHandle table,
                                            final TupleDomain<VastColumnHandle> predicates,
                                            final VastTransaction tx,
                                            final VastTraceToken token,
                                            final ConnectorSession session)
    {
        final List<URI> endpoints = getDataEndpoints(session);
        int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);
        final VastSplitContext getSizeContext = new VastSplitContext(
                0xffffffffL - 3, 1, 1, rowGroupsPerSubSplit);
        final VastSplit split = new VastSplit(null, endpoints, getSizeContext,
                null, predicates, token.toString());

        VastTableHandle getSizeTableHandle = new VastTableHandle(
                table.getSchemaName(), table.getTableName(),
                table.getHandleID(), false, false);
        List<VastColumnHandle> vastColumnHandles = (predicates
                .getDomains()
                .orElseThrow())
                .entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isAll())
                .map(entry -> VastColumnHandle.fromField(
                        entry.getKey().getField()))
                .collect(Collectors.toList());
        getSizeTableHandle.setColumnHandlesCache(vastColumnHandles);

        try (ConnectorPageSource pageSource = vastPageSourceProvider.createPageSource(
                (VastTransactionHandle) tx, session, split, getSizeTableHandle,
                emptyList(), DynamicFilter.EMPTY)) {
            SourcePage page = getFirstAvailablePage(pageSource);
            if (page != null) {
                return (long) page.getPositionCount() * (1L << 16);
            }
            throw new RuntimeException();
        }
        catch (IOException e) {
            throw new RuntimeException(format("GetTableSize(%s) failed: %s",
                    getTraceStr(table, token), e.getMessage()), e);
        }
    }

    private static SourcePage getFirstAvailablePage(ConnectorPageSource pageSource)
    {
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page != null) {
                return page;
            }
        }
        return null;
    }

    private static String getTraceStr(VastTableHandle table,
                                      VastTraceToken token)
    {
        return format("%s:%s", token, "getTableSize:" + table.getTableName());
    }
}
