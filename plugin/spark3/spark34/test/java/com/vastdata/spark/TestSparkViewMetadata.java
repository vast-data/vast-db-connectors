package com.vastdata.spark;

import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.VastPayloadSerializer;
import com.vastdata.client.schema.VastViewMetadata;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestSparkViewMetadata
{
    @Test
    public void testToVastCreateViewContext()
            throws IOException
    {
        String[] namespace = {"foo", "bar"};
        Identifier ident = Identifier.of(namespace, "baz");
        Seq<Tuple2<String, Option<String>>> cols = (Seq<Tuple2<String, Option<String>>>) Seq$.MODULE$.<Tuple2<String, Option<String>>>empty();
        Map<String, String> props = Map$.MODULE$.empty();
        StructType testSchema = new StructType(new StructField[] {
                new StructField("b", DataTypes.BooleanType, true, Metadata.empty())
        });
        SparkViewMetadata unit = new SparkViewMetadata(ident, false, true, Option.empty(),
                cols, props, "select a from ndb.buc.schem.tab", testSchema, "ndb", namespace);
        VastViewMetadata vastVastViewMetadata = unit.toVastCreateViewContext();
        Schema schema = vastVastViewMetadata.schema();
        VectorSchemaRoot metadata = vastVastViewMetadata.metadata();

        byte[] serializedSchema = VastPayloadSerializer.getInstanceForSchema().apply(schema).get();
        byte[] serializedViewDetails = VastPayloadSerializer.getInstanceForRecordBatch().apply(metadata).get();
        int bothLengths = serializedSchema.length + serializedViewDetails.length;

        ArrowSchemaUtils util = new ArrowSchemaUtils();
        byte[] bytes = util.serializeCreateViewRequestBody(serializedSchema, serializedViewDetails);
        System.out.printf("serializedSchema = %s, serializedViewDetails = %s, bothLengths = %s, bytes = %s%n", serializedSchema.length, serializedViewDetails.length, bothLengths, bytes.length);

        try (VectorSchemaRoot vectorSchemaRoot = util.deserializeCreateViewRequestBody(bytes)) {
            assertEquals(vectorSchemaRoot.getRowCount(), 1);
        }
    }
}
