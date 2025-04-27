package com.example;

import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.NoExternalRowIdColumnException;
import com.vastdata.vdb.sdk.VastSdkConfig;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class Main {
	static String schemaName = "vastdb/s2";
	static Field x = new Field("x", FieldType.nullable(new ArrowType.Utf8()), null);
	static Field y = new Field("y", FieldType.nullable(new ArrowType.Utf8()), null);
	static List<Field> exampleFieldsWithRowId = List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD, x, y);
	static List<Field> exampleFields = List.of(x, y);
	static Schema exampleSchema = new Schema(exampleFields);

	private static VastSdk sdk;

	public static void main(String[] args) {
		try {
			setup();

			nonExistingTableExample();
			nonExistingSchemaExample();
			rowsReturningSanityExample();
			rowsReturningTwoPutsExample();

			timeManyOneRowPuts();
			timeManyMultiRowPuts();
			timeManyGets();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createVastSdk() {
		String endpoint = System.getenv("ENDPOINT");
		String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
		String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");

		if (awsAccessKeyId == null || awsSecretAccessKey == null) {
			throw new IllegalArgumentException("AWS credentials must be provided");
		}

		URI uri = URI.create(endpoint);

		VastSdkConfig config = new VastSdkConfig(uri,
				uri.toString(),
				awsAccessKeyId,
				awsSecretAccessKey);

		HttpClient httpClient = new JettyHttpClient();

		sdk = new VastSdk(httpClient, config);
	}

	private static void setup() throws VastException {
		createVastSdk();
		internalCreateSchema();
	}

	private static void nonExistingTableExample() throws NoExternalRowIdColumnException {
		System.out.println("Running nonExistingTableExample");
		Table table = sdk.getTable("vastdb/s", "non-existent-table");
		try {
			table.loadSchema();
		} catch (RuntimeException e) {
			assert e.getMessage().contains("VastUserException: Failed to execute request LIST_COLUMNS") &&
					e.getMessage().contains("path /vastdb/s/non-existent-table not found")
					: "expected table not found";
		}
		assert false : "excected table not found";
	}

	private static void nonExistingSchemaExample() throws NoExternalRowIdColumnException {
		System.out.println("Running test testNonExistingSchema");
		Table table = sdk.getTable("vastdb/non-existent-schema", "non-existent-table");
		try {
			table.loadSchema();
		} catch (RuntimeException e) {
			assert e.getMessage().contains("VastUserException: Failed to execute request LIST_COLUMNS") &&
					e.getMessage().contains(
							"path /vastdb/non-existent-schema/non-existent-table not found")
					: "expected table not found";
		} catch (Exception e) {
			assert e.getMessage().contains("VastUserException: Failed to execute request LIST_COLUMNS") &&
					e.getMessage().contains(
							"path /vastdb/non-existent-schema/non-existent-table not found")
					: "expected table not found";

		}

		assert false : "expected table not found";
	}

	private static VectorSchemaRoot exampleRecord() {
		RootAllocator allocator = new RootAllocator();

		VarCharVector xVector = new VarCharVector("x", allocator);
		xVector.allocateNew(2);
		xVector.set(0, new Text("x0"));
		xVector.set(1, new Text("x1"));
		xVector.setValueCount(2);

		VarCharVector yVector = new VarCharVector("y", allocator);
		yVector.allocateNew(2);
		yVector.set(0, new Text("y0"));
		yVector.set(1, new Text("y1"));
		yVector.setValueCount(2);

		ArrayList<FieldVector> fieldVectors = new ArrayList<>();
		fieldVectors.add(xVector);
		fieldVectors.add(yVector);

		return new VectorSchemaRoot(exampleSchema, fieldVectors, 2);
	}

	private static void rowsReturningSanityExample() throws VastException {
		System.out.println("Running rowsReturningSanityExample");
		String tableName = "tab1";

		internalCreateTable(schemaName, tableName);

		Table table = sdk.getTable(schemaName, tableName);
		table.loadSchema();

		VectorSchemaRoot insertedRowIds = table.put(exampleRecord());

		UInt8Vector vec = (UInt8Vector) insertedRowIds.getVector(0);
		assert vec.get(0) != 0 || vec.get(1) != 1 : "row ids not as expected";

		VectorSchemaRoot row0 = table.get(null, 0);
		VectorSchemaRoot row1 = table.get(null, 1);
	}

	private static void rowsReturningTwoPutsExample() throws VastException {
        System.out.println("Running rowsReturningTwoPutsExample");

	String tableName = "tab2";
	internalCreateTable(schemaName, tableName);

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        VectorSchemaRoot insertedRowIds1 = table.put(exampleRecord());

        UInt8Vector vec1 = (UInt8Vector) insertedRowIds1.getVector(0);
	assert vec1.get(0) != 0 || vec1.get(1) != 1: "row ids not as expected";

        VectorSchemaRoot insertedRowIds2 = table.put(exampleRecord());

        UInt8Vector vec2 = (UInt8Vector) insertedRowIds2.getVector(0);
	assert vec2.get(0) == 2 && vec2.get(1) == 3: "row ids not as expected";
    }

	private static void timeManyOneRowPuts() throws VastException {
		System.out.println("Running timeManyOneRowPuts");
		String tableName = "tab3";
		internalCreateTable(schemaName, tableName);

		Table table = sdk.getTable(schemaName, tableName);
		table.loadSchema();

		RootAllocator allocator = new RootAllocator();

		VarCharVector xVector = new VarCharVector("x", allocator);
		xVector.allocateNew(1);
		xVector.set(0, "x".getBytes());
		xVector.setValueCount(2);

		VarCharVector yVector = new VarCharVector("y", allocator);
		yVector.allocateNew(1);
		yVector.set(0, "y".getBytes());
		yVector.setValueCount(1);

		ArrayList<FieldVector> fieldVectors = new ArrayList<>();
		fieldVectors.add(xVector);
		fieldVectors.add(yVector);

		final int numRows = 1000;

		ArrayList<VectorSchemaRoot> vsrs = new ArrayList<>();
		for (int i = 0; i < numRows; i++) {
			vsrs.add(new VectorSchemaRoot(table.getSchema(), fieldVectors, 1));
		}

		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numRows; i++) {
			table.put(vsrs.get(i));
		}
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("Elapsed time for timeManyOneRowPuts: " + elapsedTime + " ms");
	}

	private static void timeManyMultiRowPuts() throws VastException {
		System.out.println("Running timeManyMultiRowPuts");
		String tableName = "tab4";
		internalCreateTable(schemaName, tableName);

		Table table = sdk.getTable(schemaName, tableName);
		table.loadSchema();

		RootAllocator allocator = new RootAllocator();

		VarCharVector xVector = new VarCharVector("x", allocator);
		xVector.allocateNew(1000);
		for (int i = 0; i < 1000; i++) {
			xVector.set(i, "x".getBytes());
		}
		xVector.setValueCount(1000);

		VarCharVector yVector = new VarCharVector("y", allocator);
		yVector.allocateNew(1000);
		for (int i = 0; i < 1000; i++) {
			xVector.set(i, "y".getBytes());
		}
		yVector.setValueCount(1000);

		ArrayList<FieldVector> fieldVectors = new ArrayList<>();
		fieldVectors.add(xVector);
		fieldVectors.add(yVector);

		final int numRows = 1000;

		ArrayList<VectorSchemaRoot> vsrs = new ArrayList<>();
		for (int i = 0; i < numRows; i++) {
			vsrs.add(new VectorSchemaRoot(table.getSchema(), fieldVectors, 1000));
		}

		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numRows; i++) {
			table.put(vsrs.get(i));
		}
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("Elapsed time for timeManyMultiRowPuts: " + elapsedTime + " ms");
	}

	private static void timeManyGets() throws VastException {
		System.out.println("Running timeManyGets");
		String tableName = "tab5";

		internalCreateTable(schemaName, tableName);

		Table table = sdk.getTable(schemaName, tableName);
		table.loadSchema();

		VectorSchemaRoot insertedRowIds = table.put(exampleRecord());

		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			table.get(null, 0);
		}
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("Elapsed time for timeManyGets: " + elapsedTime + " ms");
	}

	//
	//
	//
	// This is internal code and not part of the Java SDK API.
	//
	//
	//
	private static void internalCreateTable(String schemaName, String tableName) throws VastException {
		VastClient client = sdk.getVastClient();
		TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
		SimpleVastTransaction tx = transactionsManager
				.startTransaction(new StartTransactionContext(false, false));
		client.createTable(tx, new CreateTableContext(schemaName, tableName, exampleFieldsWithRowId, null, null));
		transactionsManager.commit(tx);
	}

	private static void internalCreateSchema()throws VastException {
		VastClient client = sdk.getVastClient();

		TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
		SimpleVastTransaction tx = transactionsManager
				.startTransaction(new StartTransactionContext(false, false));

		if (client.schemaExists(tx, schemaName)) {
			Stream<String> tables = client.listTables(tx, schemaName, 20);
			Iterable<String> tableIterable = tables::iterator;
			for (String tableName : tableIterable) {
				client.dropTable(tx, new DropTableContext(schemaName, tableName));
			}

			client.dropSchema(tx, schemaName);
		}

		client.createSchema(tx, schemaName,
				new VastMetadataUtils().getPropertiesString(Collections.emptyMap()));

		transactionsManager.commit(tx);

	}
}
