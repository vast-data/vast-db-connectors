package com.vastdata.trino.rowid;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Int;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

public final class ArrowTypeToTrinoRowIDTypeFunction implements Function<ArrowType, Type>
{
    public static final ArrowTypeToTrinoRowIDTypeFunction INSTANCE = new ArrowTypeToTrinoRowIDTypeFunction();
    private static final DecimalType DECIMAL_38_0_TYPE = DecimalType.createDecimalType(38, 0);

    private ArrowTypeToTrinoRowIDTypeFunction() {}

    @Override
    public Type apply(ArrowType arrowType)
    {
        if (arrowType.getTypeID().equals(Int)) {
            return BIGINT;
        }
        else if (arrowType instanceof Decimal decimal) {
            int precision = decimal.getPrecision();
            int scale = decimal.getScale();
            if (precision == 38 && scale == 0) {
                return DECIMAL_38_0_TYPE;
            }
        }
        throw new IllegalArgumentException("Unsupported arrow type for ROW_ID field: " + arrowType);
    }
}
