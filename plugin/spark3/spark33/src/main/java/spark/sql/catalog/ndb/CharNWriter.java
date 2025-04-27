/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;

public class CharNWriter
        extends ArrowFieldWriter implements ICharNWriter
{
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
        String rawValue = input.getUTF8String(ordinal).toString();
        setPaddedValue(rawValue, typeLength, vector);
    }

    @Override
    public int getCount()
    {
        return count();
    }
}
