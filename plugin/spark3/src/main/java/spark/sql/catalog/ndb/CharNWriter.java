/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.base.CharMatcher;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Verify.verify;

public class CharNWriter
        extends ArrowFieldWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(CharNWriter.class);
    private final FixedSizeBinaryVector vector;
    private final int typeLength;

    public CharNWriter(FixedSizeBinaryVector vector, int typeLength) {
        this.vector = vector;
        this.typeLength = typeLength;
    }

    @Override
    public ValueVector valueVector()
    {
        return vector;
    }

    @Override
    public void setNull()
    {
        vector.setNull(count());
    }

    @Override
    public void setValue(SpecializedGetters input, int ordinal)
    {
        UTF8String utf8String = input.getUTF8String(ordinal);
        String padded = rightPadSpaces(utf8String.toString(), typeLength);
        verify(CharMatcher.ascii().matchesAllOf(padded), "CHAR type supports only ASCII charset");
        vector.set(count(), padded.getBytes(StandardCharsets.UTF_8));
    }

    private static String rightPadSpaces(String value, int length)
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
