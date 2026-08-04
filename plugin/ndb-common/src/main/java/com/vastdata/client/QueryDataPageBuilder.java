/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.metrics.DataResponseParserMetrics;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A builder for creating pages of query data.
 *
 * @param <T> The type of the page to build.
 * @param <C> The type of the prefill column.
 */
public interface QueryDataPageBuilder<T, C>
{
    /**
     * Adds a {@link VectorSchemaRoot} to the builder.
     *
     * @param root The {@link VectorSchemaRoot} to add.
     * @return This builder.
     */
    QueryDataPageBuilder<T, C> add(VectorSchemaRoot root);

    /**
     * Builds the page.
     *
     * @param metrics The metrics to update.
     * @return The built page.
     */
    T build(DataResponseParserMetrics metrics);

    /**
     * Clears the builder.
     */
    void clear();

    /**
     * Checks if this is the final page. final page represented the end of the
     * data stream and contains no data. If the method is not implemented it
     * will be assumed that this is not the final page.
     *
     * @return {@code true} if this is the final page, {@code false} otherwise.
     */
    default boolean isFinalPage()
    {
        return false;
    }

    /**
     * Builds a page with prefilled data.
     *
     * @param rows The number of rows in the page.
     * @param prefillColumn The column to prefill.
     * @return The built page. The page mush contains only a single result
     * repeated for the specified number of rows. The page must contain only the
     * prefill column and no other columns. If the method is not implemented a
     * {@link UnsupportedOperationException} will be thrown.
     */
    default T buildPrefillPage(int rows, PrefillColumn<C> prefillColumn)
    {
        throw new UnsupportedOperationException("Prefill is not supported");
    }
}
