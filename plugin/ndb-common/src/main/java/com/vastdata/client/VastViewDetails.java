package com.vastdata.client;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Optional;

public final class VastViewDetails {
    public final String originalSql;
    public final String catalog;
    public final String schema;
    public final List<Field> columns;
    public final Optional<String> comment;
    public final String owner;
    public final boolean runAsInvoker;

    public VastViewDetails(final String originalSql,
                           final String catalog,
                           final String schema,
                           final List<Field> columns,
                           final Optional<String> comment,
                           final String owner,
                           final boolean runAsInvoker)
    {
        this.originalSql = originalSql;
        this.catalog = catalog;
        this.schema = schema;
        this.columns = columns;
        this.comment = comment;
        this.owner = owner;
        this.runAsInvoker = runAsInvoker;
    }
}
