/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.requiresTSConversion;

public final class VectorAdaptorFactory
{
    private VectorAdaptorFactory() {}

    public static Optional<VectorAdaptor> forField(Field field)
    {
        if (field.equals(ROW_ID_FIELD)) {
            return Optional.of(new RowIDVectorAdaptor());
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.FixedSizeBinary)) {
            return Optional.of(new CharNVectorAdaptor());
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Timestamp) && requiresTSConversion(field)) {
            return Optional.of(new TimestampVectorAdaptor());
        }
        return Optional.empty();
    }
}
