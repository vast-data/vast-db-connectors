/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RowColumnSecurityResponse {
    final List<String> rowFilters;
    final Set<String> allowedColumns;
    final Set<String> deniedColumns;
    final Map<String, String> maskedColumns;

    public RowColumnSecurityResponse(final List<String> rowFilters,
                                     final Set<String> allowedColumns,
                                     final Set<String> deniedColumns,
                                     final Map<String, String> maskedColumns)
    {
        this.rowFilters = rowFilters;
        this.allowedColumns = allowedColumns;
        this.deniedColumns = deniedColumns;
        this.maskedColumns = maskedColumns;
    }

    public List<String> getRowFilters()
    {
        return rowFilters;
    }

    public Set<String> getAllowedColumns()
    {
        return allowedColumns;
    }

    public Set<String> getDeniedColumns()
    {
        return deniedColumns;
    }

    public Map<String, String> getMaskedColumns()
    {
        return maskedColumns;
    }

    @Override
    public String toString() {
        return "RowColumnSecurityResponse{" +
                "rowFilters=" + rowFilters +
                ", allowedColumns=" + allowedColumns +
                ", deniedColumns=" + deniedColumns +
                ", maskedColumns=" + maskedColumns +
                '}';
    }
}
