package com.vastdata.trino.rowid;

import com.vastdata.client.rowid.RowIDFieldFactory;
import com.vastdata.trino.VastTableHandle;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Int;

public class TrinoRowIDFieldFactory
        extends RowIDFieldFactory<VastTableHandle>
{
    private TrinoRowIDFieldFactory()
    {
        super(TrinoRowIDTypeFactory.INSTANCE);
    }

    public static final TrinoRowIDFieldFactory INSTANCE = new TrinoRowIDFieldFactory();

    public static Type getRowIDTypeFromArrowType(ArrowType arrowType)
    {
        if (arrowType.getTypeID().equals(Int)) {
            return BIGINT;
        }
        else {
            return DecimalType.createDecimalType(38, 0);
        }
    }
}
