package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.function.UnaryOperator;

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

public final class SparkVectorAdaptorUtil
{
    private SparkVectorAdaptorUtil() {}

    public static void convertFixedSizeBinaryIntoVarchar(FixedSizeBinaryVector src, VarCharVector dst)
    {
        final int valueCount = src.getValueCount();
        final int byteWidth = src.getByteWidth();
        ArrowBuf data = src.getDataBuffer();
        ArrowBuf validity = src.getValidityBuffer();
        try (ArrowBuf offsets = dst.getAllocator().buffer((long) (valueCount + 1) * OFFSET_WIDTH)) {
            int offset = 0;
            int index = 0;
            for (int i = 0; i <= valueCount; ++i) {
                offsets.setInt(index, offset);
                index += OFFSET_WIDTH;
                offset += byteWidth;
            }
            ArrowFieldNode node = new ArrowFieldNode(valueCount, src.getNullCount());
            // See https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout for buffer order definition
            dst.loadFieldBuffers(node, ImmutableList.of(validity, offsets, data));
            dst.setValueCount(valueCount);
        }
    }

    public static void convertNonMicroTSVectorToMicroTSVector(TimeStampVector src, TimeStampMicroTZVector dst)
    {
        int valueCount = src.getValueCount();
        dst.allocateNew(valueCount);
        UnaryOperator<Long> valueOperator = getVastTimestampToSparkValueAdaptor(src);
        for (int i = 0; i < valueCount; i++) {
            Long originalValue = (Long) src.getObject(i);
            if (originalValue != null) {
                Long adaptedValue = valueOperator.apply(originalValue);
                dst.set(i, adaptedValue);
            }
            else {
                dst.setNull(i);
            }
        }
        dst.setValueCount(valueCount);
    }

    private static UnaryOperator<Long> getVastTimestampToSparkValueAdaptor(TimeStampVector src)
    {
        if (src instanceof TimeStampSecTZVector) {
            return value -> value * 1000000;
        }
        else if (src instanceof TimeStampMilliTZVector) {
            return value -> value * 1000;
        }
        else if (src instanceof TimeStampNanoTZVector) {
            return value -> value / 1000;
        }
        else {
            throw new RuntimeException("Unsupported timestamp type: " + src.getClass());
        }
    }

    public static UnaryOperator<Long> getSparkTimestampToVastValueAdaptor(TimeStampVector src)
    {
        if (src instanceof TimeStampSecTZVector) {
            return value -> value / 1000000;
        }
        else if (src instanceof TimeStampMilliTZVector) {
            return value -> value / 1000;
        }
        else if (src instanceof TimeStampNanoTZVector) {
            return value -> value * 1000;
        }
        else {
            throw new RuntimeException("Unsupported timestamp type: " + src.getClass());
        }
    }

    public static boolean requiresTSConversion(Field field)
    {
        if (field.getType() instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) field.getType();
            return !timestamp.getUnit().equals(TimeUnit.MICROSECOND);
        }
        else {
            return false;
        }
    }
}
