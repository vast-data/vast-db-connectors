package com.vastdata.spark;

import com.vastdata.client.schema.VastViewMetadata;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.ArrayData$;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import spark.sql.catalog.ndb.TypeUtil;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.vastdata.client.ParsedURL.compose;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_COLUMN_ALIASES;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_COLUMN_COMMENTS;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_COMMENT;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_CURRENT_CATALOG;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_CURRENT_NAMESPACE;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_PROPERTIES;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_QUERY_COLUMN_NAMES;
import static com.vastdata.client.schema.VastViewMetadata.FIELD_SQL;
import static com.vastdata.client.schema.VastViewMetadata.defaultViewColumn;
import static com.vastdata.client.schema.VastViewMetadata.indexOf;
import static java.lang.String.format;
import static spark.sql.catalog.ndb.UnsupportedTypes.UNSUPPORTED_TYPE_PREDICATE;

public class SparkViewMetadata
{
    private static final Logger LOG = LoggerFactory.getLogger(SparkViewMetadata.class);

    private final Identifier identifier;
    private final boolean replace;
    private final boolean allowExisting;
    private final Option<String> comment;
    private final Map<String, String> properties;
    private final String origRawViewSqlDefinition;
    private final StructType schema;
    private final String currentCatalog;
    private final String[] currentNamespace;
    private final UTF8String[] columnComments;
    private final UTF8String[] aliases;

    public SparkViewMetadata(Identifier identifier, boolean replace, boolean allowExisting, Option<String> comment,
            Map<String, String> properties, String origRawViewSqlDefinition, StructType schema, String currentCatalog,
            String[] currentNamespace, String[] columnComments, String[] aliases)
    {
        this.identifier = identifier;
        this.replace = replace;
        this.allowExisting = allowExisting;
        this.comment = comment;
        this.properties = properties;
        this.origRawViewSqlDefinition = origRawViewSqlDefinition;
        this.schema = schema;
        this.currentCatalog = currentCatalog;
        this.currentNamespace = currentNamespace;
        this.columnComments = Arrays.stream(columnComments).map(UTF8String::fromString).toArray(UTF8String[]::new);
        this.aliases = Arrays.stream(aliases).map(UTF8String::fromString).toArray(UTF8String[]::new);
    }

    public SparkViewMetadata(Identifier identifier, boolean replace, boolean allowExisting,
            Option<String> comment, Seq<Tuple2<String, Option<String>>> userSpecifiedColumns,
            Map<String, String> properties, String origRawViewSqlDefinition, StructType schema,
            String currentCatalog, String[] currentNamespace)
    {
        this.identifier = identifier;
        this.replace = replace;
        this.allowExisting = allowExisting;
        this.comment = comment;
        this.properties = properties;
        this.origRawViewSqlDefinition = origRawViewSqlDefinition;
        this.schema = schema;
        this.currentCatalog = currentCatalog;
        this.currentNamespace = currentNamespace;
        if (!userSpecifiedColumns.isEmpty()) {
            int size = schema.size();
            SparkViewUserDefinedColumnsValidator.validateNumberOfUserDefinedColumns(size, userSpecifiedColumns.size());
            columnComments = new UTF8String[size];
            aliases = new UTF8String[size];
            AtomicInteger i = new AtomicInteger();
            userSpecifiedColumns.foreach((tup) -> {
                int currIndex = i.getAndIncrement();
                aliases[currIndex] = UTF8String.fromString(tup._1());
                if (tup._2().isDefined()) {
                    columnComments[currIndex] = UTF8String.fromString(tup._2().get());
                }
                return null;
            });
        }
        else {
            aliases = new UTF8String[0];
            columnComments = new UTF8String[0];
        }
    }

    public boolean isReplace()
    {
        return replace;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    public Identifier getIdentifier()
    {
        return identifier;
    }

    public VastViewMetadata toVastCreateViewContext()
    {
        ArrowWriter arrowWriter;
        BufferAllocator writerAllocator = VastArrowAllocator.writeAllocator().newChildAllocator("CreateSparkViewContext", 0, Long.MAX_VALUE);
        VectorSchemaRoot currentRoot = VectorSchemaRoot.create(VastViewMetadata.VIEW_DETAILS_SCHEMA, writerAllocator);
        try {
            arrowWriter = TypeUtil.getArrowSchemaWriter(currentRoot);
        }
        catch (Exception any) {

            throw toRuntime(any);
        }

        GenericInternalRow internalRow = new GenericInternalRow(VastViewMetadata.VIEW_DETAILS_SCHEMA.getFields().size());

        internalRow.update(indexOf(FIELD_SQL), UTF8String.fromString(origRawViewSqlDefinition));

        internalRow.update(indexOf(FIELD_CURRENT_CATALOG), UTF8String.fromString(currentCatalog));

        internalRow.update(indexOf(FIELD_COMMENT), UTF8String.fromString(comment.isEmpty() ? "" : comment.get()));

        ArrayData currentNameSpaceAsArrayData = ArrayData$.MODULE$.allocateArrayData(-1, currentNamespace.length,
                "Error during CurrentNamespace array creation");
        for (int i = 0; i < currentNamespace.length; i++) {
            currentNameSpaceAsArrayData.update(i, UTF8String.fromString(currentNamespace[i]));
        }
        internalRow.update(indexOf(FIELD_CURRENT_NAMESPACE), currentNameSpaceAsArrayData);

        ArrayData emptyArray = ArrayData$.MODULE$.allocateArrayData(-1, 0,
                "Error during empty array creation");
        internalRow.update(indexOf(FIELD_QUERY_COLUMN_NAMES), emptyArray);

        ArrayData columnAliasesArray = aliases.length > 0 ? ArrayData.toArrayData(aliases): emptyArray;
        internalRow.update(indexOf(FIELD_COLUMN_ALIASES), columnAliasesArray);

        ArrayData columnCommentsArray = columnComments.length > 0 ? ArrayData.toArrayData(columnComments): emptyArray;
        internalRow.update(indexOf(FIELD_COLUMN_COMMENTS), columnCommentsArray);

        MapData emptyMap = new ArrayBasedMapData(emptyArray, emptyArray);
        internalRow.update(indexOf(FIELD_PROPERTIES), emptyMap);

        try {
            arrowWriter.write(internalRow);
        }
        finally {
            arrowWriter.finish();
        }
        List<Field> viewFieldsSafe = Arrays.stream(schema.fields()).map(column -> {
            try {
                Field field = TypeUtil.sparkFieldToArrowField(column);
                if (UNSUPPORTED_TYPE_PREDICATE.test(field)) {
                    LOG.warn("Defaulting to binary column for unsupported view column: {}. Arrow field: {}", column, field);
                    return defaultViewColumn(column.name());
                }
                if (!field.isNullable()) {
                    LOG.warn("Adapting nullability of non-null view column: {}, Arrow field: {}", column, field);
                    return new Field(field.getName(), FieldType.nullable(field.getType()), field.getChildren());
                }
                return field;
            }
            catch (Exception any) {
                LOG.warn(format("Converting spark column to arrow field failed. Defaulting to binary column for view column: %s", column), any);
                return defaultViewColumn(column.name());
            }
        }).collect(Collectors.toList());
        Schema viewSchemaSafe = new Schema(viewFieldsSafe);
        LOG.warn("Creating VastViewMetadata with safe schema: {}", viewSchemaSafe);
        return new VastViewMetadata(compose(identifier.namespace()), identifier.name(), currentRoot, viewSchemaSafe);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SparkViewMetadata.class.getSimpleName() + "[", "]")
                .add("identifier=" + identifier)
                .add("replace=" + replace)
                .add("allowExisting=" + allowExisting)
                .add("comment=" + comment)
                .add("properties=" + properties)
                .add("origRawViewSqlDefinition='" + origRawViewSqlDefinition + "'")
                .add("schema=" + schema)
                .add("currentCatalog='" + currentCatalog + "'")
                .add("currentNamespace=" + Arrays.toString(currentNamespace))
                .add("columnComments=" + Arrays.toString(columnComments))
                .add("aliases=" + Arrays.toString(aliases))
                .toString();
    }
}