package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Set;
import java.util.function.Predicate;

public class UnsupportedTypes
{
    // possible types from ArrowUtils.toArrowType that are unsupported by vast
    private static final Set<ArrowType.ArrowTypeID> unsupportedNDBTypes = ImmutableSet.of(ArrowType.ArrowTypeID.Interval,
            ArrowType.ArrowTypeID.Duration,
            ArrowType.ArrowTypeID.Null);
    public static final Predicate<Field> UNSUPPORTED_TYPE_PREDICATE = field -> unsupportedNDBTypes.contains(field.getType().getTypeID());
}
