/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.base.CharMatcher;
import org.apache.arrow.vector.FixedSizeBinaryVector;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Verify.verify;

public interface ICharNWriter
{
    int getCount();

    default void setPaddedValue(String rawValue, int typeLength, FixedSizeBinaryVector vector)
    {
        String padded = rightPadSpaces(rawValue, typeLength);
        verify(CharMatcher.ascii().matchesAllOf(padded), "CHAR type supports only ASCII charset");
        vector.set(getCount(), padded.getBytes(StandardCharsets.UTF_8));
    }

    default String rightPadSpaces(String value, int length)
    {
        // Works for ASCII translations only.
        // Spark does not support Char(N) so values are unpadded varchars, so pad short values and fail longer values
        // VAST requires padded values.
        if (length > value.length()) {
            return String.format("%1$-" + length + "s", value);
        }
        else if (length == value.length()) {
            return value;
        }
        else {
            throw new IllegalArgumentException("Value too long");
        }
    }
}
