package com.vastdata.spark;

import static java.lang.String.format;

public final class SparkViewUserDefinedColumnsValidator
{
    public static void validateNumberOfUserDefinedColumns(int schemaSize, int userDefinedColumnsSize)
    {
        if (userDefinedColumnsSize != 0 && userDefinedColumnsSize != schemaSize) {
            throw new IllegalArgumentException(
                    format("The number of user defined view columns (%s) does not match the number of columns in query schema (%s)",
                            userDefinedColumnsSize, schemaSize));
        }
    }
}
