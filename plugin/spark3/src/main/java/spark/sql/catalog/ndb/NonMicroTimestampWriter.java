/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonMicroTimestampWriter
        extends ArrowFieldWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(NonMicroTimestampWriter.class);
    private final TimeStampVector vector;
    private final Types.MinorType minorType;

    public NonMicroTimestampWriter(TimeStampVector vector) {
        this.vector = vector;
        minorType = vector.getMinorType();
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
        vector.set(count(), input.getLong(ordinal));
    }
}
