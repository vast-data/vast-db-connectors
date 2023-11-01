/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

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

        int[] dataChannel = range(0, columnCount).toArray();
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
        Optional<Page> deletePage= Optional.empty();

        if (insertPositionCount > 0) {
            LOG.debug("MERGE insert page: \n%s", page);
            insertPage = Optional.of(page.getPositions(insertPositions, 0, insertPositionCount).getColumns(dataChannel));
        }

        if (deletePositionCount > 0) {
            //delete should only have row_ids columns
            LOG.debug("MERGE delete page: \n%s", page);
            deletePage = Optional.of(page.getPositions(deletePositions, 0, deletePositionCount).getColumns(columnCount + 1));
        }

        if (updatePositionCount > 0) {
            LOG.debug("MERGE update page: \n%s", page);
            int[] updateChannel = new int[columnCount + 1];
            updateChannel[0] = columnCount + 1;
            for (int i = 0; i < columnCount; i++) {
                updateChannel[i + 1] = i;
            }
            updatePage = Optional.of(page.getPositions(updatePositions, 0, updatePositionCount).getColumns(updateChannel));
        }

        return new VastMergePage(updatePage, deletePage, insertPage);
    }
}