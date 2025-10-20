package com.vastdata.trino;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorViewDefinition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class VastConnectorViewDefinition extends ConnectorViewDefinition {
    private final Map<String, String> properties;

    public VastConnectorViewDefinition(final String originalSql,
                                       final Optional<String> catalog,
                                       final Optional<String> schema,
                                       final List<ViewColumn> columns,
                                       final Optional<String> comment,
                                       final Optional<String> owner,
                                       final boolean runAsInvoker,
                                       final List<CatalogSchemaName> path,
                                       final Map<String, String> properties)
    {
        super(originalSql, catalog, schema, columns, comment, owner, runAsInvoker, path);
        this.properties = Collections.unmodifiableMap(properties);
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }
}
