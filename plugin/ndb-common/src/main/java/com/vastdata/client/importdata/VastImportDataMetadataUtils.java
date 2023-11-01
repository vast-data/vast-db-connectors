/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Optional;

import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.error.VastExceptionFactory.userException;
import static java.lang.String.format;

public final class VastImportDataMetadataUtils
{
    public static final String IMPORT_DATA_HIDDEN_COLUMN_NAME = "$parquet_file_path";
    public static final String IMPORT_DATA_TABLE_NAME_SUFFIX = " vast.import_data";
    public static final Field IMPORT_DATA_HIDDEN_FIELD = new Field(IMPORT_DATA_HIDDEN_COLUMN_NAME, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);

    public static final String BIG_CATALOG_SCHEMA_PREFIX = BIG_CATALOG_BUCKET_NAME + "/";
    public static final String BIG_CATALOG_SCHEMA_SUFFIX = "/vast_big_catalog_schema";
    public static final String BIG_CATALOG_TABLE_NAME = "vast_big_catalog_table";
    private static final String BIG_CATALOG_TABLE_SEARCH_PATH_PREFIX = BIG_CATALOG_TABLE_NAME + ":";

    private VastImportDataMetadataUtils() {}

    public static boolean isImportDataTableName(String origTableName)
    {
        return origTableName.indexOf(IMPORT_DATA_TABLE_NAME_SUFFIX) > 0;
    }

    public static String getTableNameForAPI(String origTableName)
    {
        return isImportDataTableName(origTableName) ?
                origTableName.substring(0, origTableName.indexOf(IMPORT_DATA_TABLE_NAME_SUFFIX)).trim() :
                origTableName;
    }

    public static String getTablePath(String schemaName, String origTableName)
    {
        return format("/%s/%s", schemaName, getTableNameForAPI(origTableName));
    }

    public static Optional<String> getBigCatalogSearchPath(String schemaName, String tableName)
    {
        // Note: we should also handle snapshot-based query, e.g. schemaName == 'vast-big-catalog-bucket/.snapshot/2022-11-30_07_53_27/vast_big_catalog_schema'
        if (schemaName.startsWith(BIG_CATALOG_SCHEMA_PREFIX) && schemaName.endsWith(BIG_CATALOG_SCHEMA_SUFFIX) && tableName.startsWith(BIG_CATALOG_TABLE_SEARCH_PATH_PREFIX)) {
            return Optional.of(tableName.substring(BIG_CATALOG_TABLE_SEARCH_PATH_PREFIX.length()));
        }
        else {
            return Optional.empty();
        }
    }

    public static int getImportDataHiddenColumnIndex(List<Field> fields)
    {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
                return i;
            }
        }
        throw toRuntime(userException(format("Could not find %s column in list: %s", IMPORT_DATA_HIDDEN_COLUMN_NAME, fields)));
    }
}
