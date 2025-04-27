/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import java.util.stream.IntStream;

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

    public static Seq<String> adaptTableIdentifiersToRowLevelOp(Seq<String> origIdentifiers)
    {
        Builder<String, List<String>> newIdentifiersBuilder = List.newBuilder();
        IntStream.range(0, origIdentifiers.size() - 1).forEachOrdered(i -> newIdentifiersBuilder.$plus$eq(origIdentifiers.apply(i)));
        String adaptedTableName = adaptTableNameToRowLevelOp(origIdentifiers.apply(origIdentifiers.size() - 1));
        newIdentifiersBuilder.$plus$eq(adaptedTableName);
        return newIdentifiersBuilder.result();
    }

    private static String adaptTableNameToRowLevelOp(String tableName)
    {
        return format("%s %s", tableName, ROW_LEVEL_OP_IDENTIFIER_SUFFIX);
    }
}
