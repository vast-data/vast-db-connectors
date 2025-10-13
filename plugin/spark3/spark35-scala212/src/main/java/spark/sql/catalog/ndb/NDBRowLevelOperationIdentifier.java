/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import static java.lang.String.format;

public final class NDBRowLevelOperationIdentifier
{
    private static final String ROW_LEVEL_OP_IDENTIFIER_SUFFIX = "VAST_DB_ROW_LEVEL_OP";

    private NDBRowLevelOperationIdentifier() {}

    private static int indexOfSuffix(String tableName)
    {
        return tableName.indexOf(ROW_LEVEL_OP_IDENTIFIER_SUFFIX);
    }

    public static boolean isForRowLevelOp(String tableName)
    {
        return indexOfSuffix(tableName) > 0;
    }

    public static String trimTableNameFromRowLevelOpSuffix(String tableName)
    {
        return isForRowLevelOp(tableName) ?
                tableName.substring(0, indexOfSuffix(tableName)).trim() :
                tableName;
    }

    public static String adaptTableNameToRowLevelOp(String tableName)
    {
        return format("%s %s", tableName, ROW_LEVEL_OP_IDENTIFIER_SUFFIX);
    }
}
