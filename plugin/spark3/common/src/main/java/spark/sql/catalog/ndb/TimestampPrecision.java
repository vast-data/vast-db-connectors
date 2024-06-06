/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

public enum TimestampPrecision
{
    SECONDS(0),
    MILLISECONDS(3),
    MICROSECONDS(6),
    NANOSECONDS(9);

    private final int precisionID;

    TimestampPrecision(int precisionID)
    {
        this.precisionID = precisionID;
    }

    public static TimestampPrecision fromID(int precision)
    {
        switch (precision) {
            case 0:
                return SECONDS;
            case 3:
                return MILLISECONDS;
            case 6:
                return MICROSECONDS;
            case 9:
                return NANOSECONDS;
        }
        return null;
    }

    public int getPrecisionID()
    {
        return precisionID;
    }
}
