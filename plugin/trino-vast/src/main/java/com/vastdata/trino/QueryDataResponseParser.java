/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.BaseQueryDataResponseParser;
import com.vastdata.client.PrefillColumn;
import com.vastdata.client.QueryDataPageBuilder;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.metrics.DataResponseParserMetrics;
import com.vastdata.client.tx.VastTraceToken;
import io.airlift.log.Logger;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public class QueryDataResponseParser
        extends BaseQueryDataResponseParser<SourcePage, VastColumnHandle>
{
    private static final Logger LOG = Logger.get(QueryDataResponseParser.class);

    private final QueryDataResponseSchemaConstructor querySchema;
    private final VastConfig vastConfig;
    private final List<PrefillColumn<VastColumnHandle>> prefillColumns;

    public QueryDataResponseParser(ShapingLoggerFactory shapingLoggerFactory,
                                   VastConfig vastConfig,
                                   VastTraceToken traceToken,
                                   QueryDataResponseSchemaConstructor querySchema,
                                   List<PrefillColumn<VastColumnHandle>> prefillColumns,
                                   VastDebugConfig debugConfig,
                                   QueryDataPagination pagination,
                                   Optional<Long> limitTotalRows,
                                   Optional<Long> columnByteLimit)
    {
        super(shapingLoggerFactory, traceToken, querySchema.getServerFields(),
                pagination, limitTotalRows, debugConfig, columnByteLimit);
        this.querySchema = querySchema;
        this.vastConfig = vastConfig;
        this.prefillColumns = prefillColumns;
        LOG.debug(
                "QueryData(%s) QueryDataResponseParser init: schema=%s, requested fields=%s",
                traceStr, querySchema, serverFields);
    }

    @Override
    protected QueryDataPageBuilder<SourcePage, VastColumnHandle> createPageBuilder(
            Schema requestedSchema)
    {
        return new VastPageBuilder(shapingLoggerFactory, vastConfig, traceStr,
                requestedSchema);
    }

    @Override
    protected SourcePage joinPages(List<SourcePage> pages,
                                   QueryDataPageBuilder<SourcePage, VastColumnHandle> pageBuilder)
    {
        verify(!pages.isEmpty());
        int rows = pages.getFirst().getPositionCount();
        int columnCount = pages
                .stream()
                .mapToInt(SourcePage::getChannelCount)
                .sum();
        Block[] blocks = new Block[columnCount + prefillColumns.size()];
        int projectBlockIndex = 0;
        int pageIndex = 0;
        Map<Integer, PrefillColumn<VastColumnHandle>> prefillColumnMap = prefillColumns
                .stream()
                .collect(Collectors.toMap(PrefillColumn::getProjectionIndex,
                        Function.identity()));
        for (int j = 0; j < blocks.length; j++) {
            if (prefillColumnMap.containsKey(projectBlockIndex)) {
                PrefillColumn<VastColumnHandle> prefillColumn = prefillColumnMap.get(
                        projectBlockIndex);
                if (rows > 0) {
                    metrics.incPrefillBlocks();
                }
                SourcePage sourcePage = pageBuilder.buildPrefillPage(rows,
                        prefillColumn);
                if (sourcePage.getChannelCount() != 1) {
                    throw new IllegalStateException(String.format(
                            "Expected prefill page to have exactly 1 channel, but got %s",
                            sourcePage.getChannelCount()));
                }
                blocks[projectBlockIndex] = sourcePage.getBlock(0);
                projectBlockIndex += 1;
            }
            else {
                SourcePage page = pages.get(pageIndex);
                verify(page.getPositionCount() == rows,
                        "QueryData(%s): row count mismatch: %s != %s", traceStr,
                        page.getPositionCount(), rows);
                for (int i = 0; i < page.getChannelCount(); i++) {
                    blocks[projectBlockIndex] = page.getBlock(i);
                    projectBlockIndex += 1;
                }
                pageIndex += 1;
            }
        }
        SourcePage page = querySchema.construct(blocks, rows);
        metrics.addTotalPositions(page.getPositionCount());
        return page;
    }

    public DataResponseParserMetrics getMetrics()
    {
        return metrics;
    }
}
