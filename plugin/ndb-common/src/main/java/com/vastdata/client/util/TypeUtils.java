/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import com.vastdata.ShapingLogger;
import com.vastdata.ShapingLoggerFactory;
import io.airlift.log.Logger;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class TypeUtils
{
    private static final Logger LOG = Logger.get(TypeUtils.class);
    private final ShapingLogger shapingLogger;

    public TypeUtils(ShapingLoggerFactory shapingLoggerFactory)
    {
        this.shapingLogger = shapingLoggerFactory.getInstance(getClass(), LOG);
    }

    public Field adaptMapToList(Field field, Optional<String> printPrefix)
    {
        if (!field.getType().isComplex()) {
            if (LOG.isDebugEnabled()) {
                String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
                shapingLogger.info(
                        "%sField %s is not a Map - returning primitive field",
                        prefixToPrint, field);
            }
            return field;
        }
        List<Field> fieldChildren = field.getChildren();
        String name = field.getName();
        ArrowType typeInstance;
        if (field.getType() instanceof ArrowType.Map) {
            typeInstance = ArrowType.List.INSTANCE;
            if (LOG.isDebugEnabled()) {
                String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
                shapingLogger.debug(
                        "%sField %s is Map - adapting to List from children=%s",
                        prefixToPrint, field, fieldChildren);
            }
        }
        else if (field.getType() instanceof ArrowType.Struct) {
            typeInstance = ArrowType.Struct.INSTANCE;
            if (LOG.isDebugEnabled()) {
                String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
                shapingLogger.debug(
                        "%sField %s is Struct - adapting children if needed: %s",
                        prefixToPrint, field, fieldChildren);
            }
        }
        else if (field.getType() instanceof ArrowType.List) {
            typeInstance = ArrowType.List.INSTANCE;
            if (LOG.isDebugEnabled()) {
                String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
                shapingLogger.debug(
                        "%sField %s is List - adapting children if needed: %s",
                        prefixToPrint, field, fieldChildren);
            }
        }
        else {
            throw new UnsupportedOperationException(
                    format("Unsupported complex type: %s", field));
        }
        Field adaptedField = new Field(name,
                new FieldType(field.isNullable(), typeInstance,
                        field.getDictionary(), field.getMetadata()),
                fieldChildren
                        .stream()
                        .map(c -> adaptMapToList(c, printPrefix))
                        .collect(Collectors.toList()));
        if (LOG.isDebugEnabled()) {
            String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
            shapingLogger.debug("%sReturning adapted field: %s", prefixToPrint,
                    adaptedField);
        }
        return adaptedField;
    }
}
