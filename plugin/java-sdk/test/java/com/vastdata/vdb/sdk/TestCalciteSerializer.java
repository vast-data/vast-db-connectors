/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb.sdk;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.schema.EnumeratedSchema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCalciteSerializer {

    private FlatBufferBuilder builder;
    private EnumeratedSchema schema;

    @BeforeMethod
    public void setup()
    {
        this.builder = new FlatBufferBuilder(128);
        this.schema = new EnumeratedSchema(List.of(
                Field.nullable("rowid", new ArrowType.Int(32, true)),
                Field.nullable("cint", new ArrowType.Int(32, true)),
                Field.nullable("cboolean", new ArrowType.Bool()),
                Field.nullable("csmallint", new ArrowType.Int(16, true)),
                Field.nullable("cstr", new ArrowType.Utf8())));
    }

    @Test(expectedExceptions = RuntimeException.class)
    void serializeInvalidStatementThrowsRuntimeException()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema, "INVALID SQL");
        serializer.serialize(builder);
    }

    @Test
    void testGetSchemaTableName()
    {
        assertEquals(CalciteSerializer.getTableName("SELECT cint FROM schema.t"), "schema.t");
    }

    @Test
    void getProjectedColumns()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema, "SELECT cint FROM t limit 10");
        assertEquals(serializer.getProjectedColumns(), List.of("cint"));
    }

    @Test
    void getStarProjectedColumns()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema, "SELECT * FROM t");
        List<String> allColumns = schema.getSchema().getFields().stream().map(Field::getName).toList();
        assertEquals(serializer.getProjectedColumns(), allColumns);
    }

    @Test
    void testSerializeEmptyPredicate()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT cint FROM t");
        assertEquals(serializer.serialize(builder), 348);
    }

    @Test
    void testSerializeIntegerPredicate()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT cint FROM t where cint = 10");
        assertEquals(serializer.serialize(builder), 436);
    }

    @Test
    void testSerializeVarcharPredicate()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT cint FROM t where cstr = 'hello'");
        assertEquals(serializer.serialize(builder), 432);
    }

    @Test
    void testSerializeBooleanPredicate()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT cboolean FROM t where cboolean = true");
        assertEquals(serializer.serialize(builder), 432);
    }

    @Test
    void testSerializeSmallintPredicate()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT csmallint FROM t where csmallint <= 5");
        assertEquals(serializer.serialize(builder), 448);
    }

    @Test
    void testSerializeMultiOr()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT csmallint FROM t where csmallint <= 5 OR csmallint <= 6 OR csmallint <= 7 OR csmallint <= 8");
        assertEquals(serializer.serialize(builder), 864);
    }

    @Test
    void testLimit()
    {
        CalciteSerializer serializer = new CalciteSerializer(schema,"SELECT * FROM t LIMIT 10");
        assertTrue(serializer.getLimit().equals(Optional.of(10)));
    }
}
