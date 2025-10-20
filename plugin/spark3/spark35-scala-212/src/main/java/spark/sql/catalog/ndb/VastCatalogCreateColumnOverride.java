/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static spark.sql.catalog.ndb.TypeUtil.SPARK_ROW_ID_FIELD;

public class VastCatalogCreateColumnOverride
        implements CatalogExtension
{
    private static final Logger LOG = LoggerFactory.getLogger(VastCatalogCreateColumnOverride.class);

    private final VastCatalog vastCatalog;

    public VastCatalogCreateColumnOverride(VastCatalog vastCatalog)
    {
        this.vastCatalog = vastCatalog;
    }

    @Override
    public Set<TableCatalogCapability> capabilities()
    {
        return vastCatalog.capabilities();
    }

    @Override
    public Identifier[] listTables(String[] namespace)
            throws NoSuchNamespaceException
    {
        return vastCatalog.listTables(namespace);
    }

    @Override
    public Table loadTable(Identifier ident)
            throws NoSuchTableException
    {
        return vastCatalog.loadTable(ident);
    }

    @Override
    public Table loadTable(Identifier ident, String version)
            throws NoSuchTableException
    {
        return vastCatalog.loadTable(ident, version);
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp)
            throws NoSuchTableException
    {
        return vastCatalog.loadTable(ident, timestamp);
    }

    @Override
    public void invalidateTable(Identifier ident)
    {
        vastCatalog.invalidateTable(ident);
    }

    @Override
    public boolean tableExists(Identifier ident)
    {
        return vastCatalog.tableExists(ident);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException
    {
        try {
            schema.apply(SPARK_ROW_ID_FIELD.name());
            StructField[] fields = IntStream.range(0, schema.size()).mapToObj(schema::apply).filter(f -> !SPARK_ROW_ID_FIELD.name().equals(f.name())).toArray(StructField[]::new);
            LOG.info("Removed {} column from new table {} schema: {}", SPARK_ROW_ID_FIELD.name(), ident, fields);
            return vastCatalog.createTable(ident, new StructType(fields), partitions, properties);
        }
        catch (IllegalArgumentException ignored) {
            return vastCatalog.createTable(ident, schema, partitions, properties);
        }
    }

    @Override
    public Table createTable(Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException
    {
        Column[] newColumns = Arrays.stream(columns).filter(f -> !SPARK_ROW_ID_FIELD.name().equals(f.name())).toArray(Column[]::new);
        StructType schema = CatalogV2Util.v2ColumnsToStructType(newColumns);
        return vastCatalog.createTable(ident, schema, partitions, properties);
    }

    @Override
    public boolean useNullableQuerySchema()
    {
        return vastCatalog.useNullableQuerySchema();
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes)
            throws NoSuchTableException
    {
        return vastCatalog.alterTable(ident, changes);
    }

    @Override
    public boolean dropTable(Identifier ident)
    {
        return vastCatalog.dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident)
            throws UnsupportedOperationException
    {
        return vastCatalog.purgeTable(ident);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException
    {
        vastCatalog.renameTable(oldIdent, newIdent);
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options)
    {
        // do nothing
    }

    @Override
    public String name()
    {
        return vastCatalog.name();
    }

    @Override
    public String[] defaultNamespace()
    {
        return vastCatalog.defaultNamespace();
    }

    @Override
    public String[][] listNamespaces()
            throws NoSuchNamespaceException
    {
        return vastCatalog.listNamespaces();
    }

    @Override
    public String[][] listNamespaces(String[] namespace)
            throws NoSuchNamespaceException
    {
        return vastCatalog.listNamespaces(namespace);
    }

    @Override
    public boolean namespaceExists(String[] namespace)
    {
        return vastCatalog.namespaceExists(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException
    {
        return vastCatalog.loadNamespaceMetadata(namespace);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException
    {
        vastCatalog.createNamespace(namespace, metadata);
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException
    {
        vastCatalog.alterNamespace(namespace, changes);
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException
    {
        return vastCatalog.dropNamespace(namespace, cascade);
    }

    @Override
    public Identifier[] listFunctions(String[] namespace)
            throws NoSuchNamespaceException
    {
        return vastCatalog.listFunctions(namespace);
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident)
            throws NoSuchFunctionException
    {
        return vastCatalog.loadFunction(ident);
    }

    @Override
    public boolean functionExists(Identifier ident)
    {
        return vastCatalog.functionExists(ident);
    }

    @Override
    public void setDelegateCatalog(CatalogPlugin delegate)
    {
        // do nothing
    }
}
