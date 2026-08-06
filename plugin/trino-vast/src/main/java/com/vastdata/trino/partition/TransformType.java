/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.partition;

public enum TransformType
{
    IDENTITY, YEAR, MONTH, DAY, HOUR, BUCKET, TRUNCATE
}
