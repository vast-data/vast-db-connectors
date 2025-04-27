package com.vastdata.spark;

import com.google.common.collect.Streams;
import com.vastdata.client.ArrowQueryDataSchemaHelper;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.spark.adaptor.SparkVectorAdaptorFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class CommonVastColumnarBatchReader<T extends AutoCloseable>
{
    private static final Logger LOG = LoggerFactory.getLogger(CommonVastColumnarBatchReader.class);

    private final VastClient vastClient;
    protected final Optional<Integer> limit;
    protected final VastSplitContext split;
    private final List<URI> dataEndpoints;
    private final VastRetryConfig retryConfig;
    private final QueryDataPagination pagination;
    private final FlatBufferSerializer projectionSerializer;
    private final FlatBufferSerializer predicateSerializer;
    private final SimpleVastTransaction tx;
    private final VastTraceToken token;
    private final String schemaName;
    private final String tableName;
    private final EnumeratedSchema enumeratedSchema;
    private final Schema projectionSchema;
    private final boolean forAlter;
    private final BufferAllocator allocator;
    private final Function<VectorSchemaRoot, T> arrowToSparkResultAdaptor;
    private final Function<T, Integer> batchSizeFunction;
    private final VastSparkTransactionsManager transactionsManager;
    private final boolean autoClosable;
    private final VastSchedulingInfo schedulingInfo;
    private final Map<String, String> extraQueryParams;
    private Optional<Integer> pageSize;
    private final boolean enableSortedProjections;
    private QueryDataResponseParser parser;
    private T current;
    private long totalRows;
    private long totalIdleFetchTime;
    private long totalIdleGetTime;
    private long totalFetchTime;
    private int emptyPages = 0;
    private final ArrayList<Long> pageSizes = new ArrayList<>();
    private long lastAccessTime = -1;


    CommonVastColumnarBatchReader(VastClient vastClient, Integer limit, VastSplitContext split, VastConfig config,
            FlatBufferSerializer projectionSerializer, FlatBufferSerializer predicateSerializer,
            SimpleVastTransaction tx, VastTraceToken token,
            String schemaName, String tableName, EnumeratedSchema enumeratedSchema,
            Schema projectionSchema, boolean forAlter, BufferAllocator allocator,
            Function<VectorSchemaRoot, T> arrowToSparkResultAdaptor,
            Function<T, Integer> batchSizeFunction,
            VastSparkTransactionsManager transactionsManager, boolean autoClosable,
            VastSchedulingInfo schedulingInfo, Map<String, String> extraQueryParams)
    {
        this.vastClient = vastClient;
        this.limit = Optional.ofNullable(limit);
        this.split = split;
        dataEndpoints = config.getDataEndpoints();
        retryConfig = new VastRetryConfig(config.getRetryMaxCount(), config.getRetrySleepDuration());
        pageSize = Optional.of(config.getQueryDataRowsPerPage());
        enableSortedProjections = config.isEnableSortedProjections();
        pagination = new QueryDataPagination(config.getNumOfSubSplits());
        this.projectionSerializer = projectionSerializer;
        this.predicateSerializer = predicateSerializer;
        this.tx = tx;
        this.token = token;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.enumeratedSchema = enumeratedSchema;
        this.projectionSchema = projectionSchema;
        this.forAlter = forAlter;
        this.allocator = allocator;
        this.arrowToSparkResultAdaptor = arrowToSparkResultAdaptor;
        this.batchSizeFunction = batchSizeFunction;
        this.transactionsManager = transactionsManager;
        this.autoClosable = autoClosable;
        this.schedulingInfo = schedulingInfo;
        this.extraQueryParams = extraQueryParams;
        LOG.info("{} new batch reader: {} ", token, split);
    }

    private void fetchNextBatch()
    {
        try {
            AtomicReference<URI> usedDataEndpoint = new AtomicReference<>(); // can be used for sending UPDATE/DELETE to the same endpoint as SELECT
            VastDebugConfig debugConfig = new VastDebugConfig(false, false); // TODO: allow setting via config

            Optional<Long> currentLimit = limit.map(value -> Math.max(0, value - totalRows));
            if (currentLimit.isPresent() && currentLimit.get() < pageSize.get()) {
                pageSize = currentLimit.map(Math::toIntExact);
            }
            Optional<String> bigCatalogSearchPath = Optional.empty();
            Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                ArrowQueryDataSchemaHelper schemaHelper = ArrowQueryDataSchemaHelper.deconstruct(token, projectionSchema, new SparkVectorAdaptorFactory());
                parser = new QueryDataResponseParser(token, schemaHelper, debugConfig, pagination, currentLimit, allocator);
                return new QueryDataResponseHandler(parser::parse, token);
            };
            vastClient.queryData(
                    tx, token, schemaName, tableName, enumeratedSchema.getSchema(),
                    projectionSerializer, predicateSerializer, handlerSupplier, usedDataEndpoint, split, schedulingInfo, dataEndpoints,
                    retryConfig, pageSize, bigCatalogSearchPath, pagination, enableSortedProjections, extraQueryParams);
        }
        catch (Exception e) {
            throw toRuntime(e);
        }
    }

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
                        try {
                            current.close();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(format("%s Failed closing current batch", token), e);
                        }
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
                    current = arrowToSparkResultAdaptor.apply(root);
                    totalRows += batchSizeFunction.apply(current);
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

    public T get()
    {
        long now = System.currentTimeMillis();
        long waitDuration = now - lastAccessTime;
        LOG.debug("{} Time between next & get: {}", token, waitDuration);
        totalIdleGetTime += waitDuration;
        lastAccessTime = now;
        return requireNonNull(current, "current is null");
    }

    public void close()
    {
        LOG.info("{} close: {} totalRows={}, totalFetchTime={}, totalIdleFetchTime={}, totalIdleGetTime={}",
                token, split, totalRows, totalFetchTime, totalIdleFetchTime, totalIdleGetTime);
        Optional<RuntimeException> toThrow = Optional.empty();
        if (autoClosable) {
            try {
                this.transactionsManager.commit(this.tx);
            }
            catch (RuntimeException any) {
                LOG.error(format("%s: Failed committing transaction: %s", this.token, this.tx), any);
                toThrow = Optional.of(any);
            }
        }
        if (nonNull(current)) {
            try {
                current.close();
            }
            catch (Exception e) {
                throw new RuntimeException(format("%s Failed closing current batch", token), e);
            }
            current = null;
        }
        if (nonNull(parser)) {
            Streams.stream(parser).forEach(VectorSchemaRoot::close);
        }
        if (!forAlter) {
            IllegalStateException allocationException = verifyBufferAllocation(token.toString(), allocator);
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
    public static IllegalStateException verifyBufferAllocation(String traceToken, BufferAllocator allocator)
    {
        IllegalStateException allocationException = null;
        long allocated = allocator.getAllocatedMemory();
        if (allocated != 0) {
            String msg = format("%s: %d bytes are not freed: %s", traceToken, allocated, allocator.toVerboseString());
            LOG.error(msg);
            allocationException = new IllegalStateException(msg); // TODO: consider disabling via config/session
        }
        return allocationException;
    }

    public long getTotalFetchTime()
    {
        return totalFetchTime;
    }

    public long getTotalIdleFetchTime()
    {
        return totalIdleFetchTime;
    }

    public long getTotalRows()
    {
        return totalRows;
    }

    public long getTotalIdleGetTime()
    {
        return totalIdleGetTime;
    }

    public int getEmptyPages()
    {
        return emptyPages;
    }

    public ArrayList<Long> getPageSizes()
    {
        return pageSizes;
    }
}
