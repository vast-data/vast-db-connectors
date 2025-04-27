/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static spark.sql.catalog.ndb.TypeUtil.ARRAY_ITEM_COLUMN_NAME;

public class TestTypeUtil
{
    @Test
    public void testNestedCharNArrowToSparkAndBack()
    {
        ArrowType arrowType = ArrowType.Struct.INSTANCE;
        String name = "f";
        FieldType charnType = FieldType.nullable(new ArrowType.FixedSizeBinary(3));
        List<Field> children = new ArrayList<>();
        children.add(new Field("charnfield", charnType, new ArrayList<>()));
        Field testField = new Field(name, FieldType.nullable(arrowType), children);
        StructField result = TypeUtil.arrowFieldToSparkField(testField);
        DataType dataType = result.dataType();
        assertTrue(dataType instanceof StructType);
        StructType structType = (StructType) dataType;
        assertEquals(structType.size(), 1);
        assertEquals(structType.apply(0).dataType(), new CharType(3));
        Field arrowField = TypeUtil.sparkFieldToArrowField(result);
        assertEquals(arrowField, testField);
    }

    @Test
    public void testArraySparkToArrow()
    {
        String name = "l";
        ArrayType arrayType = DataTypes.createArrayType(DataTypes.DoubleType);
        StructField testField = new StructField(name, arrayType, true, Metadata.empty());
        Field arrowField = TypeUtil.sparkFieldToArrowField(testField);
        assertEquals(arrowField.getName(), name);
        assertEquals(Iterables.getOnlyElement(arrowField.getChildren()).getName(), ARRAY_ITEM_COLUMN_NAME);
        StructField structField = TypeUtil.arrowFieldToSparkField(arrowField);
        assertEquals(structField, testField);
    }
}
