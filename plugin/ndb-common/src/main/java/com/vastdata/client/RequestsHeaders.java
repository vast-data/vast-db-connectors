/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

public enum RequestsHeaders
{
    TABULAR_TRANSACTION_ID("tabular-txid"),
    TABULAR_TRANSACTION_READ_ONLY("tabular-txro"),
    TABULAR_CASE_SENSITIVE("tabular-case-sensitive"),
    TABULAR_NEXT_KEY("tabular-next-key"),
    TABULAR_TRUNCATED("tabular-is-truncated"),
    TABULAR_MAX_KEYS("tabular-max-keys"),
    TABULAR_COLUMN_NAME("tabular-column-name"),
    TABULAR_CONTENT_LENGTH("Content-Length"),
    TABULAR_CLIENT_TAG("tabular-client-tag"),
    TABULAR_CLIENT_NAME("tabular-client-name"),
    TABULAR_API_VERSION_ID("tabular-api-version-id"),
    TABULAR_EXACT_MATCH("tabular-name-exact-match"),
    TABULAR_TRACE_TOKEN("tabular-trace-token");

    private final String headerName;

    RequestsHeaders(String headerName)
    {
        this.headerName = headerName;
    }

    public String getHeaderName()
    {
        return headerName;
    }
}
