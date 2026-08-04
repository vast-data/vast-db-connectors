/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record VastConnectorTableFunctionHandle(String query,
        boolean enforceIdentity)
        implements ConnectorTableFunctionHandle
{
    public static final String GROUPS_KEYWORD = "<groups>";
    public static final Pattern IDENTITY_PATTERN = Pattern.compile(
            format(".*%s.*", GROUPS_KEYWORD));

    public VastConnectorTableFunctionHandle
    {
        requireNonNull(query, "query must not be null");
        verify(!query.isEmpty());
    }
}
