/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class VastViewMetadata
{
    public static final String VIEW_METADATA_TABLE = "view-metadata-table";

    public static final String FIELD_SQL = "sql";
    public static final String FIELD_CURRENT_CATALOG = "current_catalog";
    public static final String FIELD_COMMENT = "comment";
    public static final String FIELD_CURRENT_NAMESPACE = "current_namespace";
    public static final String FIELD_QUERY_COLUMN_NAMES = "query_column_names";
    public static final String FIELD_COLUMN_ALIASES = "column_aliases";
    public static final String FIELD_COLUMN_COMMENTS = "column_comments";
    public static final String FIELD_PROPERTIES = "properties";

    public static final String PROPERTY_ORIGINAL_SQL = "original-sql";
    public static final String VAST_TRINO_TYPES_IDS = "vast_trino_type_ids";
    public static final String PROPERTY_OWNER = "owner";
    public static final String PROPERTY_IS_RUN_AS_INVOKER = "is-run-as-invoker";

    public static final Schema VIEW_DETAILS_SCHEMA;
    private final String schemaName;
    private final String viewName;
    private final VectorSchemaRoot metadata;
    private final Schema schema;

    public static final String NESTED_ARR_FIELD_NAME = "item";

    public static final Field SQL_FIELD = new Field(FIELD_SQL, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

    public static final Field COLUMN_ALIASES_FIELD = new Field(FIELD_COLUMN_ALIASES, FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(new Field(NESTED_ARR_FIELD_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    public static final Field COLUMN_COMMENTS_FIELD = new Field(FIELD_COLUMN_COMMENTS, FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(new Field(NESTED_ARR_FIELD_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    public static final Field COMMENT_FIELD = new Field(FIELD_COMMENT, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

    static {
        final Field currentCatalogField = new Field(FIELD_CURRENT_CATALOG, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

        final Field currentNamespaceField = new Field(FIELD_CURRENT_NAMESPACE, FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(new Field(NESTED_ARR_FIELD_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        final Field queryColumnNamesField = new Field(FIELD_QUERY_COLUMN_NAMES, FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(new Field(NESTED_ARR_FIELD_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        final List<Field> mapChildFields = Collections.singletonList(
                new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), asList(
                        new Field(MapVector.KEY_NAME, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                        new Field(MapVector.VALUE_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
                )));
        final Field propertiesField = new Field(FIELD_PROPERTIES, FieldType.nullable(new ArrowType.Map(false)), mapChildFields);
        VIEW_DETAILS_SCHEMA =  new Schema(asList(SQL_FIELD, currentCatalogField, COMMENT_FIELD, currentNamespaceField, queryColumnNamesField, COLUMN_ALIASES_FIELD, COLUMN_COMMENTS_FIELD, propertiesField));
    }


    public VastViewMetadata(String schemaName, String viewName, VectorSchemaRoot metadata, Schema schema)
    {
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.metadata = metadata;
        this.schema = schema;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getViewName()
    {
        return viewName;
    }

    public VectorSchemaRoot metadata()
    {
        return metadata;
    }

    public Schema schema()
    {
        return schema;
    }

    public static int indexOf(String fieldName)
    {
        return VastViewMetadata.VIEW_DETAILS_SCHEMA.getFields().indexOf(VastViewMetadata.VIEW_DETAILS_SCHEMA.findField(fieldName));
    }

    public static Field defaultViewColumn(String columnName)
    {
        FieldType type = new FieldType(true, new ArrowType.Binary(), null);
        return new Field(columnName, type, Collections.emptyList());
    }
}
