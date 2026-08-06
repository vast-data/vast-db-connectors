/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.page;

import com.vastdata.trino.VastColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.List;

public class PageUtil
{
    private PageUtil()
    {
    }

    // This method is probably not performant and mostly meant for debugging
    public static String toString(Page page,
                                  List<VastColumnHandle> columns,
                                  int rowsLimit)
    {
        if (rowsLimit > page.getPositionCount() || rowsLimit < 0) {
            throw new IllegalArgumentException(String.format(
                    "Page has %d rows but a limit of %d was given",
                    page.getPositionCount(), rowsLimit));
        }

        if (columns.size() != page.getChannelCount()) {
            throw new IllegalArgumentException(String.format(
                    "Page has %d columns but %d columns were given - expected to be equal",
                    page.getChannelCount(), columns.size()));
        }

        StringBuilder result = new StringBuilder();
        String[] row = new String[page.getChannelCount()];

        for (int rowIndex = 0; rowIndex < page.getPositionCount(); rowIndex++) {
            for (int colIndex = 0; colIndex < page.getChannelCount(); colIndex++) {
                Block block = page.getBlock(colIndex);

                if (block.isNull(rowIndex)) {
                    row[colIndex] = "NULL";
                    continue;
                }

                row[colIndex] = columns
                        .get(colIndex)
                        .getColumnMetadata()
                        .getType()
                        .getObjectValue(page.getBlock(colIndex), rowIndex)
                        .toString();
            }

            for (int colIndex = 0; colIndex < page.getChannelCount(); colIndex++) {
                row[colIndex] = String.format("%12s", row[colIndex]);
            }

            result.append(String.format("Row(%d): %s\n", rowIndex,
                    String.join(", ", row)));
        }

        return result.toString();
    }
}
