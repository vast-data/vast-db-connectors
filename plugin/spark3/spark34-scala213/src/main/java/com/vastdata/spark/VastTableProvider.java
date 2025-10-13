/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.error.SparkExceptionFactory;
import org.apache.parquet.Strings;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class VastTableProvider
        implements SupportsCatalogOptions
{
    //TODO - only used for extractIdentifier util at the moment. is this class needed?
    private static final Logger LOG = LoggerFactory.getLogger(VastTableProvider.class);
    public static final String CONNECTOR_NAME = "ndb";

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        LOG.debug("inferSchema: {}", options);
        // TODO: invoke ListColumns Tabular API
        return new StructType(new StructField[] {
                createStructField("a", DataTypes.StringType, true),
                createStructField("b", DataTypes.IntegerType, true),
                createStructField("c", DataTypes.DoubleType, true),
        });
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        LOG.debug("getTable: {}, {}, {}", schema, partitioning, properties);
        return new VastTable("vast_table", "vast_table", "id", schema, () -> null, false);
    }

    @Override
    public Identifier extractIdentifier(CaseInsensitiveStringMap options)
    {
        LOG.debug("extractIdentifier: {}", options);
        String table = options.get("table");
        if (Strings.isNullOrEmpty(table)) {
            throw toRuntime(SparkExceptionFactory.tableOptionNotProvided());
        }
        else {
            LOG.debug("Proceeding with table = {}", table);
        }
        try {
            // valid expected format: ndb.bucket.schema[.nested1...nestedN].table
            ParsedURL parsedURL = ParsedURL.of(table.replace('.', '/').replace("`", ""));
            String tableName = parsedURL.getTableName();
            String[] fullSchemaParts = parsedURL.getFullSchemaParts();
            // strip 'ndb' from full schema path
            String[] namespace = Arrays.copyOfRange(fullSchemaParts, 1, fullSchemaParts.length);
            Identifier of = Identifier.of(namespace, tableName);
            LOG.debug("Returning {}", of);
            return of;
        }
        catch (VastUserException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String extractCatalog(CaseInsensitiveStringMap options)
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Optional<String> extractTimeTravelTimestamp(CaseInsensitiveStringMap options)
    {
        return SupportsCatalogOptions.super.extractTimeTravelTimestamp(options);
    }

    @Override
    public Optional<String> extractTimeTravelVersion(CaseInsensitiveStringMap options)
    {
        return SupportsCatalogOptions.super.extractTimeTravelVersion(options);
    }
}
