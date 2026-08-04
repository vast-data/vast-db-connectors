/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.Lists;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.spark.adaptor.ArrowToSparkResultAdaptor;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.spark.metrics.EmptyPagesCount;
import com.vastdata.spark.metrics.EmptyPartitionsCount;
import com.vastdata.spark.metrics.PageSizeAVG;
import com.vastdata.spark.metrics.SplitFetchIdleTimeMetric;
import com.vastdata.spark.metrics.SplitFetchTimeMetric;
import com.vastdata.spark.metrics.SplitGetIdleTime;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_DEC128_ROW_ID_NONNULL;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_INT64_ROW_ID_NONNULL;
import static com.vastdata.spark.metrics.CustomTaskMetricFactory.customTaskMetric;
import static java.lang.String.format;

public class VastColumnarBatchReader
        implements PartitionReader<ColumnarBatch>
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastColumnarBatchReader.class);

    private final CommonVastColumnarBatchReader<ColumnarBatch> vastReader;

    public VastColumnarBatchReader(SimpleVastTransaction tx, int batchID,
            VastConfig vastConfig, String schemaName, String tableName,
            VastInputPartition partition, StructType schema, Integer limit,
            List<List<VastPredicate>> predicates,
            VastSchedulingInfo schedulingInfo, boolean forAlter,
            QueryDataExtraParams extraQueryParams, String endUser)
    {
        if (partition.partitionPredicate() != null) {
            ArrayList<List<VastPredicate>> tmp = new ArrayList<>(
                    predicates); // deep copy
            tmp.addAll(partition.partitionPredicate());
            predicates = tmp;
        }
        Schema projectionSchema;
        if (forAlter) {
            ArrayList<Field> fieldArrayList = Arrays
                    .asList(schema.names())
                    .contains(VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName()) ?
                    Lists.newArrayList(ROW_ID_UINT64_FIELD) :
                    Lists.newArrayList(ROW_ID_DEC128_FIELD);
            Arrays.stream(schema.fields()).filter(f -> !f
                    .name()
                    .equalsIgnoreCase(
                            VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName()) && !f
                    .name()
                    .equalsIgnoreCase(
                            VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getName())).map(
                    TypeUtil::sparkFieldToArrowField).forEach(
                    fieldArrayList::add);
            projectionSchema = new Schema(fieldArrayList);
        }
        else {
            projectionSchema = new Schema(
                    TypeUtil.sparkSchemaToArrowFieldsList(schema));
        }
        ArrowToSparkResultAdaptor<ColumnarBatch> adaptor = new ArrowToSparkResultAdaptor<>(
                ColumnarBatch.class, ColumnVector.class,
                ArrowColumnVector::new);
        List<Field> projectionSchemaFields = projectionSchema.getFields();

        BufferAllocator allocator = new RootAllocator();
        VastClient vastClient;
        VastConfig config;
        SimpleVastTransaction txToUse;
        VastSparkTransactionsManager transactionsManager;
        boolean autoClosable;
        try {
            config = vastConfig;
            vastClient = NDB.getVastClient(config);
            transactionsManager = VastSparkTransactionsManager.getInstance(
                    vastClient, new VastTransactionFactory());
            autoClosable = tx == null;
            txToUse = tx != null ? tx : transactionsManager.startTransaction(
                    null);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        VastSplitContext split = new VastSplitContext(partition.getSplitId(),
                partition.getNumOfSplits(), partition.getNumOfSubSplits(),
                config.getRowGroupsPerSubSplit());
        VastTraceToken token = txToUse.generateTraceToken(Optional.of(
                format("%s:%s", tableName,
                        batchID))); // TODO: allow user-specified trace-token
        LinkedHashSet<Field> allQueryFields = new LinkedHashSet<>(
                projectionSchemaFields);
        predicates.forEach(list -> list.forEach(vp -> allQueryFields.add(
                TypeUtil.sparkFieldToArrowField(vp.getField()))));
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(
                allQueryFields);
        LOG.debug(
                "{} VastColumnarBatchReader: {} schema={}, enumeratedSchema={}, predicates={}",
                token, partition, projectionSchema,
                enumeratedSchema.getSchema(), predicates);
        FlatBufferSerializer projectionSerializer = new SparkProjectionSerializer(
                projectionSchema, enumeratedSchema);
        FlatBufferSerializer predicateSerializer = new SparkPredicateSerializer(
                token.toString(), predicates, enumeratedSchema);
        vastReader = new CommonVastColumnarBatchReader<>(vastClient, limit,
                split, config, projectionSerializer, predicateSerializer,
                txToUse, token, schemaName, tableName, enumeratedSchema,
                projectionSchema, forAlter, allocator, adaptor,
                ColumnarBatch::numRows, transactionsManager, autoClosable,
                schedulingInfo, extraQueryParams, endUser);
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

    @Override
    public CustomTaskMetric[] currentMetricsValues()
    {
        ArrayList<CustomTaskMetric> metrics = new ArrayList<>(
                4 + vastReader.getPageSizes().size());
        metrics.add(customTaskMetric(new SplitGetIdleTime(),
                vastReader.getTotalIdleGetTime()));
        metrics.add(customTaskMetric(new SplitFetchIdleTimeMetric(),
                vastReader.getTotalIdleFetchTime()));
        metrics.add(customTaskMetric(new SplitFetchTimeMetric(),
                vastReader.getTotalFetchTime()));
        metrics.add(customTaskMetric(new EmptyPartitionsCount(),
                vastReader.getTotalRows() > 0 ? 1 : 0));
        metrics.add(customTaskMetric(new EmptyPagesCount(),
                vastReader.getEmptyPages()));
        vastReader.getPageSizes().stream().map(
                value -> customTaskMetric(new PageSizeAVG(), value)).forEach(
                metrics::add);
        return metrics.toArray(new CustomTaskMetric[0]);
    }
}
