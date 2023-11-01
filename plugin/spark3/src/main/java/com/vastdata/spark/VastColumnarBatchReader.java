/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.executor.VastRetryConfig;
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
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.spark.metrics.CustomTaskMetricFactory.customTaskMetric;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

public class VastColumnarBatchReader
        implements PartitionReader<ColumnarBatch>
{
    private static final Logger LOG = LoggerFactory.getLogger(VastColumnarBatchReader.class);

    private final String schemaName;
    private final String tableName;
    private final VastInputPartition inputPartition;
    private final VastConfig config;
    private final VastClient vastClient;
    private final VastSparkTransactionsManager transactionsManager;
    private final SimpleVastTransaction tx;
    private final VastTraceToken token;
    private final Schema projectionSchema;
    private final QueryDataPagination pagination;
    private final VastSchedulingInfo schedulingInfo;
    private final BufferAllocator allocator;
    private final Optional<Integer> limit;
    private final FlatBufferSerializer predicateSerializer;
    private final FlatBufferSerializer projectionSerializer;
    private final boolean autoclosable;
    private final EnumeratedSchema enumeratedSchema;
    private final boolean forAlter;
    private QueryDataResponseParser parser;
    private ColumnarBatch current;
    private long totalRows;
    private long totalIdleFetchTime;
    private long totalIdleGetTime;
    private long totalFetchTime;
    private int emptyPages = 0;
    private final ArrayList<Long> pageSizes = new ArrayList<>();

    public VastColumnarBatchReader(SimpleVastTransaction tx, int batchID, VastConfig vastConfig, String schemaName, String tableName,
            VastInputPartition partition, StructType schema, Integer limit, List<List<VastPredicate>> predicates,
            VastSchedulingInfo schedulingInfo, boolean forAlter)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.inputPartition = partition;
        this.forAlter = forAlter;
        if (this.forAlter) {
            this.projectionSchema = new Schema(Lists.newArrayList(ROW_ID_FIELD));
        }
        else {
            this.projectionSchema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(schema));
        }
        List<Field> projectionSchemaFields = this.projectionSchema.getFields();


        this.limit = Optional.ofNullable(limit);
        this.schedulingInfo = schedulingInfo;
        this.allocator = new RootAllocator();

        try {
            this.config = vastConfig;
            this.vastClient = NDB.getVastClient(config);
            this.transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
            this.autoclosable = tx == null;
            this.tx = tx != null ? tx : this.transactionsManager.startTransaction(new StartTransactionContext(true, true));
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        this.token = this.tx.generateTraceToken(Optional.of(format("%s:%s", tableName, batchID))); // TODO: allow user-specified trace-token
        this.pagination = new QueryDataPagination(config.getNumOfSubSplits());
        LinkedHashSet<Field> allQueryFields = new LinkedHashSet<>(projectionSchemaFields);
        predicates.forEach(list -> list.forEach(vp -> allQueryFields.add(TypeUtil.sparkFieldToArrowField(vp.getField()))));
        enumeratedSchema = new EnumeratedSchema(allQueryFields);
        LOG.debug("{} VastColumnarBatchReader: {} schema={}, enumeratedSchema={}, predicates={}", token, inputPartition, projectionSchema, enumeratedSchema.getSchema(), predicates);
        this.projectionSerializer = new SparkProjectionSerializer(projectionSchema, enumeratedSchema);
        this.predicateSerializer = new SparkPredicateSerializer(this.token.toString(), predicates, enumeratedSchema);
    }

    private void fetchNextBatch()
    {
        try {
            AtomicReference<URI> usedDataEndpoint = new AtomicReference<>(); // can be used for sending UPDATE/DELETE to the same endpoint as SELECT
            VastDebugConfig debugConfig = new VastDebugConfig(false, false); // TODO: allow setting via config

            Optional<Long> currentLimit = limit.map(value -> Math.max(0, value - totalRows));
            VastSplitContext split = new VastSplitContext(
                    inputPartition.getSplitId(),
                    inputPartition.getNumOfSplits(),
                    config.getNumOfSubSplits(),
                    config.getRowGroupsPerSubSplit());
            List<URI> dataEndpoints = config.getDataEndpoints();
            VastRetryConfig retryConfig = new VastRetryConfig(config.getRetryMaxCount(), config.getRetrySleepDuration());
            Optional<Integer> pageSize = Optional.of(config.getQueryDataRowsPerPage());
            if (currentLimit.isPresent() && currentLimit.get() < pageSize.get()) {
                pageSize = currentLimit.map(Math::toIntExact);
            }
            Optional<String> bigCatalogSearchPath = Optional.empty();
            Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                parser = new QueryDataResponseParser(token, projectionSchema, debugConfig, pagination, currentLimit, allocator);
                return new QueryDataResponseHandler(parser::parse, token);
            };
            vastClient.queryData(
                    tx, token, schemaName, tableName, enumeratedSchema.getSchema(),
                    projectionSerializer, predicateSerializer, handlerSupplier, usedDataEndpoint, split, schedulingInfo, dataEndpoints,
                    retryConfig, pageSize, bigCatalogSearchPath, pagination, config.getEnableCustomSchemaSeparator());
        }
        catch (Exception e) {
            throw toRuntime(e);
        }
    }

    private long lastAccessTime = -1;

    @Override
    public boolean next()
    {
        if (lastAccessTime > 0 ) {
            long now = System.currentTimeMillis();
            long idleFetch = now - lastAccessTime;
            LOG.debug("{} Time between get & next: {}", token, idleFetch);
            totalIdleFetchTime += idleFetch;
            lastAccessTime = now;
        }
        lastAccessTime = System.currentTimeMillis();
        try {
            while (true) {
                if (nonNull(parser) && parser.hasNext()) {
                    VectorSchemaRoot root = parser.next();
                    if (nonNull(current)) {
                        current.close();
                        current = null;
                    }
                    AtomicLong pageSizeInBytes = new AtomicLong(0);
                    if (root.getRowCount() > 0) {
                        root.getFieldVectors().forEach(vector -> pageSizeInBytes.addAndGet(vector.getBufferSize()));
                        long sizeInBytes = pageSizeInBytes.get();
                        pageSizes.add(sizeInBytes);
                    }
                    else {
                        emptyPages++;
                    }
                    Function<FieldVector, ColumnVector> mapper = new FieldToColumnMapper(allocator);
                    current = new ColumnarBatch(
                            root.getFieldVectors().stream().map(mapper).toArray(ColumnVector[]::new),
                            root.getRowCount());
                    totalRows += current.numRows();
                    return true;
                }
                if (pagination.isFinished()) {
                    LOG.debug("{} finished reading {} rows", token, totalRows);
                    return false;
                }
                // no more cached responses & pagination is not finished - send new QueryData request
                fetchNextBatch();
            }
        }
        finally {
            long now = System.currentTimeMillis();
            long pageDuration = now - lastAccessTime;
            LOG.debug("{} next duration: {}", token, pageDuration);
            totalFetchTime += pageDuration;
            lastAccessTime = now;
        }
    }

    @Override
    public ColumnarBatch get()
    {
        long now = System.currentTimeMillis();
        long waitDuration = now - lastAccessTime;
        LOG.debug("{} Time between next & get: {}", token, waitDuration);
        totalIdleGetTime += waitDuration;
        lastAccessTime = now;
        return requireNonNull(current, "current is null");
    }

    @Override
    public void close()
    {
        LOG.info("{} VastColumnarBatchReader: {} close: totalRows={}, totalFetchTime={}, totalIdleFetchTime={}, totalIdleGetTime={}",
                token, inputPartition, totalRows, totalFetchTime, totalIdleFetchTime, totalIdleGetTime);
        Optional<RuntimeException> toThrow = Optional.empty();
        if (autoclosable) {
            try {
                this.transactionsManager.commit(this.tx);
            }
            catch (RuntimeException any) {
                LOG.error(format("%s: Failed committing transaction: %s", this.token, this.tx), any);
                toThrow = Optional.of(any);
            }
        }
        if (nonNull(current)) {
            current.close();
            current = null;
        }
        if (nonNull(parser)) {
            Streams.stream(parser).forEach(VectorSchemaRoot::close);
        }
        if (!forAlter) {
            IllegalStateException allocationException = verifyBufferAllocation();
            if (allocationException != null) {
                if (!toThrow.isPresent()) {
                    toThrow = Optional.of(allocationException);
                }
                else {
                    toThrow.get().addSuppressed(allocationException);
                }
            }
        }
        toThrow.ifPresent(e -> {
            throw e;
        });
    }

    @Nullable
    IllegalStateException verifyBufferAllocation()
    {
        IllegalStateException allocationException = null;
        long allocated = allocator.getAllocatedMemory();
        if (allocated != 0) {
            String msg = format("%s: %d bytes are not freed: %s", token, allocated, allocator.toVerboseString());
            LOG.error(msg);
            allocationException = new IllegalStateException(msg); // TODO: consider disabling via config/session
        }
        return allocationException;
    }

    public BufferAllocator getAllocator()
    {
        return allocator;
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues()
    {
        ArrayList<CustomTaskMetric> metrics = new ArrayList<>(4 + pageSizes.size());
        metrics.add(customTaskMetric(new SplitGetIdleTime(), totalIdleGetTime));
        metrics.add(customTaskMetric(new SplitFetchIdleTimeMetric(), totalIdleFetchTime));
        metrics.add(customTaskMetric(new SplitFetchTimeMetric(), totalFetchTime));
        metrics.add(customTaskMetric(new EmptyPartitionsCount(), totalRows > 0 ? 1 : 0));
        metrics.add(customTaskMetric(new EmptyPagesCount(), emptyPages));
        pageSizes.stream().map(value -> customTaskMetric(new PageSizeAVG(), value)).forEach(metrics::add);
        return metrics.toArray(new CustomTaskMetric[0]);
    }
}
