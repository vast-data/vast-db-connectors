/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.Lists;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.spark.metrics.EmptyPagesCount;
import com.vastdata.spark.metrics.EmptyPartitionsCount;
import com.vastdata.spark.metrics.PageSizeAVG;
import com.vastdata.spark.metrics.SplitFetchIdleTimeMetric;
import com.vastdata.spark.metrics.SplitFetchTimeMetric;
import com.vastdata.spark.metrics.SplitGetIdleTime;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.spark.metrics.CustomTaskMetricFactory.customTaskMetric;
import static java.lang.String.format;

public class VastColumnarBatchReader
        implements PartitionReader<ColumnarBatch>
{
    private static final Logger LOG = LoggerFactory.getLogger(VastColumnarBatchReader.class);
    private static final Function<FieldVector, ColumnVector> mapper = ArrowColumnVector::new;
    private static final Function<VectorSchemaRoot, ColumnarBatch> arrowToSparkResultAdaptor = root -> new ColumnarBatch(
            root.getFieldVectors().stream().map(mapper).toArray(ColumnVector[]::new),
            root.getRowCount());

    private final BufferAllocator allocator;
    private final CommonVastColumnarBatchReader<ColumnarBatch> vastReader;

    public VastColumnarBatchReader(SimpleVastTransaction tx, int batchID,
            VastConfig vastConfig, String schemaName, String tableName,
            VastInputPartition partition, StructType schema, Integer limit,
            List<List<VastPredicate>> predicates, VastSchedulingInfo schedulingInfo,
            boolean forAlter, Map<String, String> extraQueryParams)
    {
        Schema projectionSchema;
        if (forAlter) {
            projectionSchema = new Schema(Lists.newArrayList(ROW_ID_FIELD));
        }
        else {
            projectionSchema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(schema));
        }
        List<Field> projectionSchemaFields = projectionSchema.getFields();

        this.allocator = new RootAllocator();
        VastClient vastClient;
        VastConfig config;
        SimpleVastTransaction txToUse;
        VastSparkTransactionsManager transactionsManager;
        boolean autoClosable;
        try {
            config = vastConfig;
            vastClient = NDB.getVastClient(config);
            transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
            autoClosable = tx == null;
            txToUse = tx != null ? tx : transactionsManager.startTransaction(new StartTransactionContext(true, true));
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        VastSplitContext split = new VastSplitContext(
                partition.getSplitId(),
                partition.getNumOfSplits(),
                config.getNumOfSubSplits(),
                config.getRowGroupsPerSubSplit());
        VastTraceToken token = txToUse.generateTraceToken(Optional.of(format("%s:%s", tableName, batchID))); // TODO: allow user-specified trace-token
        LinkedHashSet<Field> allQueryFields = new LinkedHashSet<>(projectionSchemaFields);
        predicates.forEach(list -> list.forEach(vp -> allQueryFields.add(TypeUtil.sparkFieldToArrowField(vp.getField()))));
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(allQueryFields);
        LOG.debug("{} VastColumnarBatchReader: {} schema={}, enumeratedSchema={}, predicates={}", token, partition, projectionSchema, enumeratedSchema.getSchema(), predicates);
        FlatBufferSerializer projectionSerializer = new SparkProjectionSerializer(projectionSchema, enumeratedSchema);
        FlatBufferSerializer predicateSerializer = new SparkPredicateSerializer(token.toString(), predicates, enumeratedSchema);
        vastReader = new CommonVastColumnarBatchReader<>(vastClient, limit, split, config,
                projectionSerializer, predicateSerializer, txToUse, token, schemaName, tableName,
                enumeratedSchema, projectionSchema, forAlter, allocator,
                arrowToSparkResultAdaptor, ColumnarBatch::numRows,
                transactionsManager, autoClosable, schedulingInfo, extraQueryParams);
    }

    @Override
    public boolean next()
    {
        return this.vastReader.next();
    }

    @Override
    public ColumnarBatch get()
    {
        return vastReader.get();
    }

    @Override
    public void close()
    {
        vastReader.close();
    }

    public BufferAllocator getAllocator()
    {
        return allocator;
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues()
    {
        ArrayList<CustomTaskMetric> metrics = new ArrayList<>(4 + vastReader.getPageSizes().size());
        metrics.add(customTaskMetric(new SplitGetIdleTime(), vastReader.getTotalIdleGetTime()));
        metrics.add(customTaskMetric(new SplitFetchIdleTimeMetric(), vastReader.getTotalIdleFetchTime()));
        metrics.add(customTaskMetric(new SplitFetchTimeMetric(), vastReader.getTotalFetchTime()));
        metrics.add(customTaskMetric(new EmptyPartitionsCount(), vastReader.getTotalRows() > 0 ? 1 : 0));
        metrics.add(customTaskMetric(new EmptyPagesCount(), vastReader.getEmptyPages()));
        vastReader.getPageSizes().stream().map(value -> customTaskMetric(new PageSizeAVG(), value)).forEach(metrics::add);
        return metrics.toArray(new CustomTaskMetric[0]);
    }
}
