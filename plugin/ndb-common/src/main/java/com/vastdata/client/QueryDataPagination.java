/* Copyright (C) Vast Data Ltd. */

package com.vastdata.client;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Multimap;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class QueryDataPagination
{
    private static final long INVALID_ROW_ID = 0xFFFFFFFFFFFFL; // (1 << 48) - 1

    // Used to aggregate multiple pagination responses, to be applied only if the whole response was parsed correctly.
    // It is needed to handle disconnections correctly during QueryData - since we want to retry the whole page again.
    public static class Update
    {
        private final Map<Integer, Long> nextRowIds;

        public Update()
        {
            nextRowIds = new HashMap<>();
        }

        public void advance(int subsplit, long nextRowId)
        {
            this.nextRowIds.merge(subsplit, nextRowId, Long::max);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("nextRowIds", nextRowIds)
                    .toString();
        }
    }

    private final Map<Integer, Long> nextRowIds;

    public QueryDataPagination(int subSplits)
    {
        this.nextRowIds = new HashMap<>(subSplits);
        IntStream.range(0, subSplits).forEach(i -> nextRowIds.put(i, 0L));
    }

    // Should be called only after QueryData response is successfully parsed
    public void advance(Update update)
    {
        update.nextRowIds.forEach((subsplit, nextRowId) -> this.nextRowIds.merge(subsplit, nextRowId, Long::max));
    }

    public boolean isFinished()
    {
        return nextRowIds.values().stream().allMatch(nextRowId -> nextRowId == INVALID_ROW_ID);
    }

    public void updateHeaders(Multimap<String, String> headers)
    {
        this.nextRowIds.forEach((subSplit, nextRowId) -> {
            if (nextRowId == INVALID_ROW_ID) {
                return; // skip subsplits that are finished
            }
            String key = format("tabular-start-row-id-%d", subSplit);
            String value = format("%d,%d", subSplit, nextRowId);
            headers.put(key, value);
        });
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("nextRowIds", nextRowIds)
                .toString();
    }
}
