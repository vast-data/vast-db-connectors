/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.VastTableProvider;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// Decorator for short "ndb" scala formatting
public class DefaultSource
        implements SupportsCatalogOptions, DataSourceRegister
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSource.class);

    private static final VastTableProvider vastTableProvider = new VastTableProvider(); //TODO - implement all here or all there

    @Override
    public Identifier extractIdentifier(CaseInsensitiveStringMap options)
    {
        LOG.debug("extractIdentifier {}", options);
        return vastTableProvider.extractIdentifier(options);
    }

    @Override
    public String extractCatalog(CaseInsensitiveStringMap options)
    {
        LOG.debug("extractCatalog {}", options);
        return "ndb";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        LOG.debug("inferSchema {}", options);
        return vastTableProvider.inferSchema(options);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        LOG.debug("getTable");
        return vastTableProvider.getTable(schema, partitioning, properties);
    }

    @Override
    public String shortName()
    {
        return VastTableProvider.CONNECTOR_NAME;
    }

    @Override
    public boolean supportsExternalMetadata()
    {
        LOG.debug("supportsExternalMetadata()");
        return SupportsCatalogOptions.super.supportsExternalMetadata();
    }
}
