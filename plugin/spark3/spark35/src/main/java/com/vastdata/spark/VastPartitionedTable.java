/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.write.VastPartitionedWriteBuilder;
import com.vastdata.spark.write.VastWriteBuilder;
import ndb.NDB;
import ndb.NDBSparkSessionExtension;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.spark.VastBatch.SINGLE_SPLIT_INPUT_PARTITION;
import static com.vastdata.spark.VastPITBatch.EST_ROW_COUNT;
import static com.vastdata.spark.predicate.VastPredicates.Equal;
import static com.vastdata.spark.predicate.VastPredicates.IsNull;
import static java.util.Objects.requireNonNull;

public class VastPartitionedTable
        extends VastTable
        implements SupportsPartitionManagement
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPartitionedTable.class);

    private final StructType pSchema;

    public VastPartitionedTable(VastCatalogUtils vastCatalogUtils,
            String schemaName, String tableName, String handleID,
            StructType schema, Transform[] partitioning,
            Supplier<VastClient> clientSupplier, boolean forImportData,
            Optional<RuntimeException> notSafeToWrite,
            Map<String, String> additionalProperties, StructType pSchema)
    {
        super(vastCatalogUtils, schemaName, tableName, handleID, schema,
                requireNonNull(partitioning, "partitioning transform is null"),
                clientSupplier, forImportData, notSafeToWrite,
                additionalProperties);
        this.pSchema = pSchema;
        LOG.info("VastPartitionedTable constructed. schema={}, pSchema={}",
                schema, pSchema);
    }

    private static StructType adaptSchemaToPIT(StructType schema)
    {
        LOG.debug("Adapting schema to PIT: {}", schema);
        StructField[] fields = Arrays.copyOf(schema.fields(),
                schema.fields().length + 1);
        fields[fields.length - 1] = EST_ROW_COUNT;
        StructType adaptedSchema = new StructType(fields);
        LOG.debug("Adapted schema to PIT: {}", adaptedSchema);
        return adaptedSchema;
    }

    @Override
    public StructType partitionSchema()
    {
        return pSchema;
    }

    @Override
    public void createPartition(InternalRow ident,
            Map<String, String> properties)
            throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(
                "Partition creation is not supported");
    }

    // This method is supposed to return false in case partition does not exist
    // But, we don't do that as it looks like Spark is making sure the partition exists before calling this method
    @Override
    public boolean dropPartition(InternalRow ident)
    {
        LOG.info("Drop partition: {}", ident);
        String endUser;

        try {
            endUser = NDBSparkSessionExtension.getSessionUser(NDB.getConfig());
        }
        catch (VastUserException e) {
            throw toRuntime("Spark connector was not initialized", e);
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(
                new Schema(TypeUtil.sparkSchemaToArrowFieldsList(this.pSchema)),
                VastArrowAllocator.writeAllocator())) {
            ArrowWriter writer = TypeUtil.getArrowSchemaWriter(root);
            writer.write(ident);
            writer.finish();
            ParsedURL url = ParsedURL.of(this.name());
            this.clientSupplier.get().dropPartitionsNonAcid(
                    this.getSchemaName(), url.getTableName(), root, endUser);
            return true;
        }
        catch (VastException e) {
            throw toRuntime("Failed to drop partitions", e);
        }
    }

    @Override
    public void replacePartitionMetadata(InternalRow ident,
            Map<String, String> properties)
            throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(
                "Partition metadata replace is not supported");
    }

    @Override
    public Map<String, String> loadPartitionMetadata(InternalRow ident)
            throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(
                "Partition metadata load is not supported");
    }

    public List<NamedReference> refForPartitioning()
    {
        Transform[] transforms = partitioning();
        List<NamedReference> nr = new ArrayList<>(transforms.length);
        for (Transform transform : transforms) {
            nr.add(transform.references()[0]);
        }
        return nr;
    }

    public List<NamedReference> partitionRefs()
    {
        return Arrays
                .stream(pSchema.fieldNames())
                .map(Expressions::column)
                .collect(Collectors.toList());
    }

    VastPITTable forPITScan()
    {
        StructType pitSchema = adaptSchemaToPIT(partitionSchema());
        return new VastPITTable(vastCatalogUtils, getSchemaName(),
                getTableMD().tableName + PIT_NAME_SUFFIX, getTableHandleID(),
                pitSchema, partitioning(), clientSupplier, Optional.empty(),
                additionalProperties);
    }

    private void pitScan(List<List<VastPredicate>> pitPredicates,
            Consumer<InternalRow> c)
            throws VastUserException
    {
        LOG.info("Creating PIT scan with schema: {}", pSchema);
        VastPITTable vastPITTable = forPITScan();
        try (PartitionReader<ColumnarBatch> pitReader = new VastPITBatch(
                vastPITTable, vastPITTable.readSchema(), pitPredicates)
                .createReaderFactory()
                .createColumnarReader(SINGLE_SPLIT_INPUT_PARTITION)) {
            while (pitReader.next()) {
                ColumnarBatch columnarBatch = pitReader.get();
                Iterator<InternalRow> rowIt = columnarBatch.rowIterator();
                while (rowIt.hasNext()) {
                    InternalRow row = rowIt.next();
                    c.accept(row);
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed closing pit reader", e);
        }
    }

    @Override
    public InternalRow[] listPartitionIdentifiers(String[] names,
            InternalRow ident)
    {
        LOG.info("listPartitionIdentifiers({}, {})", Arrays.toString(names),
                ident);

        try {
            List<List<VastPredicate>> predicates = new ArrayList<>(
                    names.length);

            for (int i = 0; i < names.length; i++) {
                int fi = pSchema.fieldIndex(names[i]);
                String pitColName = pSchema.fieldNames()[fi];
                NamedReference nr = LogicalExpressions.parseReference(
                        pitColName);
                StructField originalField = pSchema.fields()[fi];

                DataType dt = pSchema.fields()[fi].dataType();
                Predicate predicate = ident.isNullAt(i) ?
                        new IsNull(nr) :
                        new Equal(nr, dt, ident.get(i, dt));
                predicates.add(Collections.singletonList(
                        new VastPredicate(predicate, nr, originalField)));
            }

            List<InternalRow> rv = new ArrayList<>();
            pitScan(predicates, row -> rv.add(row.copy()));
            return rv.toArray(new InternalRow[0]);
        }
        catch (VastUserException e) {
            LOG.warn("Caught an exception. Returning an empty array.", e);
            return new InternalRow[0];
        }
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info)
    {
        if (notSafeToWrite.isEmpty()) {
            LOG.debug("newWriteBuilder({}.{}) {}, {}, {}", tableMD.schemaName,
                    tableMD.tableName, info.queryId(), info.schema(),
                    info.options().asCaseSensitiveMap());

            if (tableMD.isForDelete() || tableMD.isForUpdate() || tableMD.forImportData) {
                return new VastWriteBuilder(clientSupplier.get(), this);
            }

            return new VastPartitionedWriteBuilder(clientSupplier.get(), this);
        }
        else {
            final RuntimeException error = new RuntimeException(
                    notSafeToWrite.get().getMessage(), notSafeToWrite.get());
            LOG.error("Write attempt with an unsafe Spark configuration",
                    error);
            throw error;
        }
    }
}
