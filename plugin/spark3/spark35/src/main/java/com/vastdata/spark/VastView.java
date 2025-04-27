/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashMap$;
import scala.collection.mutable.ReusableBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.IntStream;

public class VastView implements View
{
    private static final Logger LOG = LoggerFactory.getLogger(VastView.class);

    private final String name_;
    private final String query_;
    private final String catalog;
    private final String[] namespace;
    private final StructType schema_;
    private final String[] queryColumnNames_;
    private final String[] columnAliases_;
    private final String[] columnComments_;
    private final Map<String, String> properties_;

    public VastView(final String name,
            final String query,
            final String catalog,
            final String comment,
            final String[] namespace,
            final StructType schema,
            final String[] queryColumnNames,
            final String[] columnAliases,
            final String[] columnComments)
    {
        this.name_ = name;
        this.query_ = query;
        this.catalog = catalog;
        this.namespace = namespace;
        this.queryColumnNames_ = queryColumnNames;
        this.columnAliases_ = columnAliases;
        this.columnComments_ = columnComments;
        this.properties_ = Strings.isNullOrEmpty(comment) ? ImmutableMap.of() : ImmutableMap.of("comment", comment);
        this.schema_ = adaptViewSchemaIfNeeded(schema, columnComments_, columnAliases_);
    }

    @Override
    public String name() {
        return name_;
    }

    @Override
    public String query() {
        return query_;
    }

    @Override
    public String currentCatalog() {
        return catalog;
    }

    @Override
    public String[] currentNamespace() {
        return namespace;
    }

    @Override
    public StructType schema() {
        return schema_;
    }

    private static StructType adaptViewSchemaIfNeeded(StructType schema, String[] columnComments, String[] columnAlias)
    {
        if (columnComments.length > 0 || columnAlias.length > 0) {
            LOG.debug("adaptViewSchemaIfNeeded for schema: {}. Using column comments and column aliases: {}, {}", schema, Arrays.toString(columnComments), Arrays.toString(columnAlias));
            StructField[] fieldsWithComments = IntStream.range(0, schema.fields().length).mapToObj(i -> {
                StructField f = schema.fields()[i];
                String comment = columnComments.length > 0 ? columnComments[i] : null;
                String alias = columnAlias.length > 0 ? columnAlias[i] : null;
                if (Strings.isNullOrEmpty(comment) && Strings.isNullOrEmpty(alias)) {
                    return f;
                }
                else {
                    Metadata newMD;
                    String newName;
                    if (Strings.isNullOrEmpty(comment)) {
                        newMD = f.metadata();
                    }
                    else {
                        scala.collection.immutable.Map<String, Object> map = f.metadata().map();
                        ReusableBuilder<Tuple2<String, Object>, HashMap<String, Object>> mdMapBuilder = HashMap$.MODULE$.newBuilder();
                        map.foreach(t -> {
                            mdMapBuilder.addOne(t);
                            return null;
                        });
                        mdMapBuilder.addOne(Tuple2.apply("comment", comment));
                        newMD = new Metadata(mdMapBuilder.result());
                    }
                    if (Strings.isNullOrEmpty(alias)) {
                        newName = f.name();
                    }
                    else {
                        newName = alias;
                    }
                    return new StructField(newName, f.dataType(), f.nullable(), newMD);
                }
            }).toArray(StructField[]::new);
            return new StructType(fieldsWithComments);
        }
        return schema;
    }

    @Override
    public String[] queryColumnNames() {
        return queryColumnNames_;
    }

    @Override
    public String[] columnAliases() {
        return columnAliases_;
    }

    @Override
    public String[] columnComments() {
        return columnComments_;
    }

    @Override
    public Map<String, String> properties() {
        return properties_;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", VastView.class.getSimpleName() + "[", "]")
                .add("name_='" + name_ + "'")
                .add("query_='" + query_ + "'")
                .add("catalog='" + catalog + "'")
                .add("namespace=" + Arrays.toString(namespace))
                .add("schema_=" + schema_)
                .add("queryColumnNames_=" + Arrays.toString(queryColumnNames_))
                .add("columnAliases_=" + Arrays.toString(columnAliases_))
                .add("columnComments_=" + Arrays.toString(columnComments_))
                .add("properties_=" + properties_)
                .toString();
    }

    public Table asTable()
    {
        return new Table() {
            @Override
            public String name()
            {
                return name_;
            }

            @Override
            public StructType schema()
            {
                return schema_;
            }

            @Override
            public Set<TableCapability> capabilities()
            {
                return ImmutableSet.of();
            }

            @Override
            public Map<String, String> properties()
            {
                return properties_;
            }

            @Override
            public String toString()
            {
                return new StringJoiner(", ", "VastViewAsTable" + "[", "]")
                        .add("name=" + name())
                        .add("schema=" + schema())
                        .add("properties=" + properties())
                        .toString();
            }
        };
    }
}
