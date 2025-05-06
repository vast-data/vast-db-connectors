/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import com.vastdata.trino.rowid.BlockComparatorFunction;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.stream.IntStream.range;

public class VastMergePage
{
    private static final Logger LOG = Logger.get(VastMergePage.class);
    private final Optional<Page> updatePage;
    private final Optional<Page> deletePage;
    private final Optional<Page> insertPage;

    private VastMergePage(Optional<Page> updatePage, Optional<Page> deletePage, Optional<Page> insertPage)
    {
        this.updatePage = updatePage;
        this.deletePage = deletePage;
        this.insertPage = insertPage;
    }

    public Optional<Page> getUpdatePage()
    {
        return updatePage;
    }

    public Optional<Page> getDeletePage()
    {
        return deletePage;
    }

    public Optional<Page> getInsertPage()
    {
        return insertPage;
    }
    public static VastMergePage createVastUpdateDeleteInsertPages(Page page, int columnCount)
    {
        int rowCount = page.getPositionCount();
        LOG.debug("createVastUpdateDeleteInsertPages: pageChannelCount: %s, rowCount: %s, columnCount: %s", page.getChannelCount(), rowCount, columnCount);

        //https://trino.io/docs/current/develop/supporting-merge.html#overview-of-merge-processing
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        Block operationBlock = page.getBlock(columnCount);

        int[] dataChannels = range(0, columnCount).toArray();
        int[] deletePositions = new int[rowCount];
        int[] insertPositions = new int[rowCount];
        int[] updatePositions = new int[rowCount];
        int insertPositionCount = 0;
        int deletePositionCount = 0;
        int updatePositionCount = 0;

        LOG.debug("Merge Operation Block:\n %s", operationBlock.toString());

        for (int position = 0; position < rowCount; position++) {
            int operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                }
                case DELETE_OPERATION_NUMBER -> {
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                }
                case UPDATE_OPERATION_NUMBER -> {
                    updatePositions[updatePositionCount] = position;
                    updatePositionCount++;
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }

        Optional<Page> updatePage = Optional.empty();
        Optional<Page> insertPage = Optional.empty();
        Optional<Page> deletePage = Optional.empty();

        // See https://trino.io/docs/current/develop/supporting-merge.html#connectormergesink-api for details
        final int rowIdChannel = columnCount + 1;

        if (insertPositionCount > 0) {
            LOG.debug("MERGE insert page: %s", page);
            insertPage = Optional.of(page.getPositions(insertPositions, 0, insertPositionCount).getColumns(dataChannels));
        }

        if (deletePositionCount > 0) {
            // delete should only have `row_id` column
            LOG.debug("MERGE delete page: %s", page);
            deletePositions = sortPositionsByRowId(deletePositions, deletePositionCount, page.getBlock(rowIdChannel));
            deletePage = Optional.of(page.getPositions(deletePositions, 0, deletePositionCount).getColumns(rowIdChannel));
        }

        if (updatePositionCount > 0) {
            LOG.debug("MERGE update page: %s", page);
            int[] updateChannels = new int[columnCount + 1];
            // `row_id` column must be the first channel in the resulting update page
            updateChannels[0] = rowIdChannel;
            for (int i = 0; i < columnCount; i++) {
                updateChannels[i + 1] = i;
            }
            updatePositions = sortPositionsByRowId(updatePositions, updatePositionCount, page.getBlock(rowIdChannel));
            updatePage = Optional.of(page.getPositions(updatePositions, 0, updatePositionCount).getColumns(updateChannels));
        }

        return new VastMergePage(updatePage, deletePage, insertPage);
    }

    // Workaround for ORION-147374 (currently VAST backend requires ascending row IDs for UPDATE/DELETE)
    static int[] sortPositionsByRowId(int[] positions, int count, Block rowIdsBlock)
    {
        Comparator<Integer> comparator = BlockComparatorFunction.INSTANCE.apply(rowIdsBlock);
        return Arrays
                .stream(positions, 0, count)
                .boxed()
                .sorted(comparator)
                .mapToInt(Integer::intValue)
                .toArray();
    }
}
