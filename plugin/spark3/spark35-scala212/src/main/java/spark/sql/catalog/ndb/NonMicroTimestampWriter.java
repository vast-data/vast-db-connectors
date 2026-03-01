/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.UnaryOperator;

public class NonMicroTimestampWriter
        extends ArrowFieldWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(NonMicroTimestampWriter.class);

    private final TimeStampVector vector;
    private final UnaryOperator<Long> sparkTimestampToVastValueAdaptor;

    public NonMicroTimestampWriter(TimeStampVector vector) {
        this.vector = vector;
        sparkTimestampToVastValueAdaptor = SparkVectorAdaptorUtil.getSparkTimestampToVastValueAdaptor(vector);
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
        long originalValue = input.getLong(ordinal);
        Long adaptedValue = this.sparkTimestampToVastValueAdaptor.apply(originalValue);
        vector.setSafe(count(), adaptedValue);
    }
}
