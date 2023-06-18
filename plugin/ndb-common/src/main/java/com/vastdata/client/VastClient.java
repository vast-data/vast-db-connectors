/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastForbiddenException;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.AlterSchemaContext;
import com.vastdata.client.schema.AlterTableContext;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.schema.VastPayloadSerializer;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import org.apache.arrow.computeir.flatbuf.Project;
import org.apache.arrow.computeir.flatbuf.Relation;
import org.apache.arrow.computeir.flatbuf.RelationImpl;
import org.apache.arrow.computeir.flatbuf.Source;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.codec.binary.Hex;
import vast_flatbuf.tabular.GetTableStatsResponse;
import vast_flatbuf.tabular.ListSchemasResponse;
import vast_flatbuf.tabular.ListTablesResponse;
import vast_flatbuf.tabular.ObjectDetails;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.amazonaws.http.HttpMethodName.DELETE;
import static com.amazonaws.http.HttpMethodName.GET;
import static com.amazonaws.http.HttpMethodName.POST;
import static com.amazonaws.http.HttpMethodName.PUT;
import static com.vastdata.client.error.VastExceptionFactory.conflictException;
import static com.vastdata.client.error.VastExceptionFactory.forbiddenException;
import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.serializationException;
import static com.vastdata.client.error.VastExceptionFactory.serverException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.error.VastExceptionFactory.userException;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpStatus.SERVICE_UNAVAILABLE;
import static java.lang.String.format;

public class VastClient
{
    private static final Logger LOG = Logger.get(VastClient.class);
    private static final String BASE = "/";

    @VisibleForTesting
    protected static final String BIG_CATALOG_BUCKET = "vast-big-catalog-bucket"; // special bucket name for BigCatalog support

    private static final RootAllocator allocator = new RootAllocator();
    private final ArrowSchemaUtils arrowSchemaUtils = new ArrowSchemaUtils();
    private final VastConfig config;
    @VisibleForTesting
    protected final HttpClient httpClient;
    private final VastDependenciesFactory dependenciesFactory;
    private final VastPayloadSerializer<Schema> schemaSerializer;
    private final VastPayloadSerializer<VectorSchemaRoot> recordBatchSerializer;

    private final RecordBatchSplitter splitter;

    private static ListSchemasResponse parseListSchemasResponse(VastResponse response)
    {
        try {
            return ListSchemasResponse.getRootAsListSchemasResponse(response.getByteBuffer());
        }
        catch (Throwable e) {
            LOG.error(e, "%s: failed to parse ListSchemasResponse: %s", response, Hex.encodeHexString(response.getByteBuffer()));
            throw e;
        }
    }

    private static final Function<ListSchemasResponse, Stream<String>> streamListSchemasResponse = r -> IntStream.range(0, r.schemasLength()).mapToObj(r::schemas).map(ObjectDetails::name);

    private static ListTablesResponse parseListTablesResponse(VastResponse response)
    {
        try {
            return ListTablesResponse.getRootAsListTablesResponse(response.getByteBuffer());
        }
        catch (Throwable e) {
            LOG.error(e, "%s: failed to parse ListTablesResponse: %s", response, Hex.encodeHexString(response.getByteBuffer()));
            throw e;
        }
    }


    private static VastStatistics parseGetTableStats(VastResponse response)
    {
        try {
            GetTableStatsResponse resp = GetTableStatsResponse.getRootAsGetTableStatsResponse(response.getByteBuffer());
            return new VastStatistics(resp.numRows(), resp.sizeInBytes());
        }
        catch (Throwable e) {
            LOG.error(e, "%s: failed to parse GetTableStatsResponse: %s", response, Hex.encodeHexString(response.getByteBuffer()));
            throw e;
        }
    }

    private static final Function<ListTablesResponse, Stream<String>> streamListTablesResponse = r -> IntStream
            .range(0, r.tablesLength())
            .mapToObj(r::tables)
            .map(ObjectDetails::name);

    private static final Function<ListTablesResponse, Stream<String>> streamListTablesResponseHandle = r -> IntStream
            .range(0, r.tablesLength())
            .mapToObj(r::tables)
            .map(ObjectDetails::handle);

    private final Function<VastResponse, IterationResult> iterationResultProvider = r -> {
        Multimap<HeaderName, String> headers = r.getHeaders();
        Collection<String> nextKeyHeader = headers.get(HeaderName.of(RequestsHeaders.TABULAR_NEXT_KEY.getHeaderName()));
        Long nextKey = Long.parseUnsignedLong(Iterables.getOnlyElement(nextKeyHeader));
        Collection<String> isTruncatedHeader = headers.get(HeaderName.of(RequestsHeaders.TABULAR_TRUNCATED.getHeaderName()));
        boolean isTruncated = Boolean.parseBoolean(Iterables.getOnlyElement(isTruncatedHeader));
        return new IterationResult() {
            @Override
            public boolean isTruncated()
            {
                return isTruncated;
            }

            @Override
            public Long getNextKey()
            {
                return nextKey;
            }
        };
    };

    @Inject
    public VastClient(@ForVast HttpClient httpClient, VastConfig config, VastDependenciesFactory dependenciesFactory)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.dependenciesFactory = dependenciesFactory;
        this.schemaSerializer = VastPayloadSerializer.getInstanceForSchema();
        this.recordBatchSerializer = VastPayloadSerializer.getInstanceForRecordBatch();
        this.splitter = new RecordBatchSplitter(config.getMaxRequestBodySize(), recordBatchSerializer);
    }

    public List<String> listBuckets(boolean includeHidden)
            throws VastIOException
    {
        LOG.debug("Listing buckets");
        Request request = new VastRequestBuilder(config, GET, BASE).build();
        VastResponse response = retryIOErrorsAndTimeouts(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandlerForListBuckets()));
        try {
            ImmutableList.Builder<String> result = ImmutableList.builder();
            result.add(new ObjectMapper().readValue(response.getBytes(), String[].class));
            if (includeHidden) {
                result.add(BIG_CATALOG_BUCKET);
            }
            return result.build();
        }
        catch (IOException e) {
            throw ioException("Failed listing buckets", e);
        }
    }

    private <T> void singleObjectFetch(VastTransaction transaction,
            Function<VastResponse, T> resultProvider,
            Consumer<T> resultConsumer,
            Requests requestName,
            String path,
            String nameToMatch)
            throws VastServerException, VastUserException
    {
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withNextKey(0L)
                .withMaxKeys(1)
                .withExactMatch(nameToMatch) // use point lookup for name
                .build();

        Request request = new VastRequestBuilder(config, GET, path, requestName.getRequestParam())
                .addHeaders(headers)
                .build();
        VastResponse response = retryIOErrorsAndTimeouts(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
        int responseStatus = response.getStatus();
        if (responseStatus == HttpStatus.NOT_FOUND.code()) {
            LOG.debug("Failed to execute request %s, path %s not found: %s", requestName.name(), path, response);
            return;
        }
        else if (responseStatus == HttpStatus.CONFLICT.code()) {
            LOG.error("Failed to execute request %s, path %s is in conflicted state: %s", requestName.name(), path, response);
            throw conflictException(format("Failed to execute request %s, path %s is in conflicted state", requestName.name(), path));
        }
        else if (responseStatus == HttpStatus.FORBIDDEN.code()) {
            LOG.error("Failed to execute request %s, path %s not allowed: %s", requestName.name(), path, response);
            throw userException(format("Failed to execute request %s, path %s not allowed", requestName.name(), path));
        }
        else if (responseStatus != OK.code()) {
            LOG.error("Failed fetching object %s: %s", nameToMatch, response);
            throw serverException(format("Failed fetching object %s: code %s", nameToMatch, responseStatus));
        }
        resultConsumer.accept(resultProvider.apply(response));
    }

    private <T> void pagedObjectsFetch(VastTransaction transaction,
            Function<VastResponse, T> resultProvider,
            Consumer<T> resultConsumer,
            Requests requestName,
            String path,
            int pageSize)
            throws VastServerException, VastUserException
    {
        boolean fetchNextPage = true;
        Long nextKey = 0L;
        while (fetchNextPage) {
            Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                    .withTransaction(transaction)
                    .withNextKey(nextKey)
                    .withMaxKeys(pageSize).build();

            Request request = new VastRequestBuilder(config, GET, path, requestName.getRequestParam())
                    .addHeaders(headers)
                    .build();
            VastResponse response = retryIOErrorsAndTimeouts(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
            int responseStatus = response.getStatus();
            if (responseStatus == OK.code()) {
                resultConsumer.accept(resultProvider.apply(response));
                IterationResult iteration = iterationResultProvider.apply(response);
                boolean truncated = iteration.isTruncated();
                if (truncated) {
                    nextKey = iteration.getNextKey();
                    LOG.debug("Received truncated page for path %s, next page key: %s", path, nextKey);
                }
                else {
                    fetchNextPage = false;
                }
            }
            else if (responseStatus == HttpStatus.NOT_FOUND.code()) {
                LOG.error("Failed to execute request %s, path %s not found: %s", requestName.name(), path, response);
                throw userException(format("Failed to execute request %s, path %s not found", requestName.name(), path));
            }
            else if (responseStatus == HttpStatus.CONFLICT.code()) {
                LOG.error("Failed to execute request %s, path %s is in conflicted state: %s", requestName.name(), path, response);
                throw conflictException(format("Failed to execute request %s, path %s is in conflicted state", requestName.name(), path));
            }
            else if (responseStatus == HttpStatus.FORBIDDEN.code()) {
                LOG.error("Failed to execute request %s, path %s not allowed: %s", requestName.name(), path, response);
                throw forbiddenException(format("Failed to execute request %s, path %s not allowed", requestName.name(), path));
            }
            else {
                LOG.error("Failed to execute request %s for path %s: %s", requestName.name(), path, response);
                throw serverException(format("Failed to execute request %s for path %s: code %s", requestName.name(), path, responseStatus));
            }
        }
    }

    public Stream<String> listAllSchemas(VastTransaction transaction, final int pageSize)
            throws VastIOException
    {
        ImmutableList.Builder<String> schemas = ImmutableList.builder();
        List<String> buckets = listBuckets(true);
        int failedBuckets = 0;
        for (String bucket : buckets) {
            try {
                listSchemas(transaction, bucket, pageSize).forEach(schemas::add);
            }
            catch (VastForbiddenException e) {
                LOG.warn(e, "Bucket %s forbids listing schemas", bucket);
                failedBuckets += 1;
            }
            catch (VastConflictException e) {
                LOG.warn(e, "Bucket %s is not of database type", bucket);
                failedBuckets += 1;
            }
            catch (VastServerException | VastUserException e) {
                throw toRuntime(e);
            }
        }
        if (failedBuckets > 0 && failedBuckets == buckets.size()) {
            // Following https://vastdata.atlassian.net/browse/ORION-77838?focusedCommentId=3056058
            throw toRuntime(new VastForbiddenException(format("Failed to list schemas in all buckets: %s", buckets)));
        }
        return schemas.build().stream();
    }

    private Stream<String> listSchemas(VastTransaction transaction, String bucket, final int pageSize)
            throws VastServerException, VastUserException
    {
        LOG.info("Listing schemas for bucket " + bucket);

        Stack<String> searchStack = new Stack<>();
        Stack<String> resultStack = new Stack<>();
        searchStack.add(bucket);

        while (!searchStack.isEmpty()) {
            String parentPath = searchStack.pop();
            if (!parentPath.equals(bucket)) {
                resultStack.add(parentPath);
            }
            Consumer<ListSchemasResponse> resultConsumer = (r) -> IntStream.range(0, r.schemasLength())
                    .mapToObj(r::schemas)
                    .map(obj -> String.format("%s/%s", parentPath, obj.name()))
                    .forEachOrdered(searchStack::add);
            try {
                pagedObjectsFetch(transaction, VastClient::parseListSchemasResponse, resultConsumer, Requests.LIST_SCHEMA, BASE + parentPath, pageSize);
            }
            catch (VastConflictException conflictException) {
                LOG.error(conflictException, "Caught conflict exception, check if path is enabled for DB API: %s", BASE + parentPath);
            }
            catch (VastForbiddenException forbiddenException) {
                LOG.error(forbiddenException, "Forbidden to list %s schema", parentPath);
            }
        }
        return resultStack.stream();
    }

    public boolean schemaExists(VastTransaction transaction, String schemaName)
            throws VastUserException, VastServerException
    {
        ParsedURL schemaUrl = ParsedURL.of(schemaName);
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), "schema name is invalid");

        String path = BASE + schemaUrl.getBucket();
        String schemaNameWithoutBucket = schemaUrl.getSchemaName();
        // hack because nested schemas. Last url part is considered a table name, so if exists needs to be added to schema name
        // TODO - add real support for nested schemas url
        if (schemaUrl.hasTable()) {
            path += BASE + schemaNameWithoutBucket;
            schemaNameWithoutBucket = schemaUrl.getTableName();
        }
        LOG.debug("Testing existence for schema: " + schemaNameWithoutBucket);

        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Consumer<ListSchemasResponse> responseConsumer = r -> streamListSchemasResponse.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListSchemasResponse, responseConsumer, Requests.LIST_SCHEMA, path, schemaNameWithoutBucket);
        LOG.debug("matching schemas: %s", results);
        return results.equals(ImmutableList.of(schemaNameWithoutBucket));
    }

    public Stream<String> listTables(VastTransaction transaction, String schema, int pageSize)
            throws VastServerException, VastUserException
    {
        String path = BASE + schema;
        LinkedBlockingQueue<String> results = new LinkedBlockingQueue<>();
        Consumer<ListTablesResponse> resultConsumer = r -> streamListTablesResponse.apply(r).forEach(results::add);
        pagedObjectsFetch(transaction, VastClient::parseListTablesResponse, resultConsumer, Requests.LIST_TABLES, path, pageSize);
        return results.stream();
    }

    public String getVastTableHandleId(VastTransaction transaction, String schemaName, String tableName)
            throws VastServerException, VastUserException
    {
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(!Strings.isNullOrEmpty(tableName), "tableName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), "schema name is invalid");
        String path = BASE + schemaName;

        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Consumer<ListTablesResponse> responseConsumer = r -> streamListTablesResponseHandle.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListTablesResponse, responseConsumer, Requests.LIST_TABLES, path, tableName);
        LOG.debug("matching table handle: %s", results);
        VerifyParam.verify(results.size() == 1,
                "Vast Table handle should contain a single element, but has " + results.size() + ".");
        return results.get(0);
    }

    public void s3PutObj(String keyName, String bucketName, String statsStr) {
        AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey());
        AwsClientBuilder.EndpointConfiguration ec = new AwsClientBuilder
                .EndpointConfiguration(config.getEndpoint().toString(), config.getRegion());
        final AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(Boolean.TRUE)
                .withEndpointConfiguration(ec)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        try {
            s3.putObject(bucketName, keyName, statsStr);
            LOG.info("Uploading object %s to Vast succeeded", keyName);
        } catch (Exception e) {
            LOG.info("Uploading object %s to Vast failed", keyName);
            throw new RuntimeException(e);
        }
    }
    public Optional<String> s3GetObj(String keyName, String bucketName) {
        AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey());
        AwsClientBuilder.EndpointConfiguration ec = new AwsClientBuilder.EndpointConfiguration(config.getEndpoint().toString(), config.getRegion());
        final AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(Boolean.TRUE)
                .withEndpointConfiguration(ec)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        try {
            String statsStr = s3.getObjectAsString(bucketName, keyName);
            LOG.info("Downloading object %s from Vast succeeded: %s", keyName, statsStr);
            return Optional.of(statsStr);

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                LOG.warn(e, "Couldn't find statistics file %s in bucket %s", keyName, bucketName);
            } else {
                LOG.warn(e, "Failed to get object, object %s not found, error message %s", keyName);
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn(e, "Failed to get object %s, error message %s", keyName);
            //TODO should we fail or return empty statistics?
            throw toRuntime(e);
        }
    }

    public boolean tableExists(VastTransaction transaction, String schemaName, String tableName)
            throws VastServerException, VastUserException
    {
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(!Strings.isNullOrEmpty(tableName), "tableName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: %s", schemaName));
        String path = BASE + schemaName;

        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Consumer<ListTablesResponse> responseConsumer = r -> streamListTablesResponse.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListTablesResponse, responseConsumer, Requests.LIST_TABLES, path, tableName);
        LOG.debug("matching tables: %s", results);
        return results.equals(ImmutableList.of(tableName));
    }

    public void createSchema(VastTransaction transaction, String schemaName, String serializedProperties)
            throws VastException
    {
        LOG.info("Creating schema %s", schemaName);
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "Schema name is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), "Schema name is invalid");
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory();
        byte[] body = arrowSchemaUtils.serializeCreateSchemaBody(serializedProperties);
        Multimap<String, String> headers = headersFactory
                .withTransaction(transaction)
                .withContentLength(body.length)
                .build();
        Request request = new VastRequestBuilder(config, POST, BASE + schemaName, Requests.CREATE_SCHEMA.getRequestParam())
                .setBody(Optional.of(body))
                .addHeaders(headers)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed creating schema name %s: %s", schemaName, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void dropSchema(VastTransaction transaction, String schemaName)
            throws VastException
    {
        LOG.info("Dropping schema %s", schemaName);
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), "schema name is invalid");
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory();
        Multimap<String, String> headers = headersFactory
                .withTransaction(transaction)
                .build();
        Request request = new VastRequestBuilder(config, DELETE, BASE + schemaName, VastRequestBuilder.EMPTY_KV_PARAMS, Requests.DROP_SCHEMA.getRequestParam())
                .addHeaders(headers)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed dropping schema %s: %s", schemaName, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void alterSchema(VastTransaction transaction, String schema, AlterSchemaContext ctx)
            throws VastException
    {
        LOG.info("Altering schema %s", schema);
        boolean anyPresent = ctx.getNewName().isPresent() || ctx.getProperties().isPresent();
        VerifyParam.verify(anyPresent, "New schema name or schema properties must be set");
        Optional<Map<String, String>> urlParams;
        if (ctx.getNewName().isPresent()) {
            String newSchemaName = ctx.getNewName().get();
            VerifyParam.verify(!Strings.isNullOrEmpty(newSchemaName), "New schema name is empty");
            urlParams = Optional.of(ImmutableMap.of("tabular-new-schema-name", newSchemaName));
        }
        else {
            urlParams = VastRequestBuilder.EMPTY_KV_PARAMS;
        }
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory();
        byte[] body = arrowSchemaUtils.serializeAlterSchemaBody(ctx);
        Multimap<String, String> headers = headersFactory
                .withTransaction(transaction)
                .withContentLength(body.length)
                .build();
        Request request = new VastRequestBuilder(config, PUT, BASE + schema, urlParams, Requests.ALTER_SCHEMA.getRequestParam())
                .setBody(Optional.of(body))
                .addHeaders(headers)
                .build();
        VastResponse response = httpClient.execute(request, VastResponseHandler.createVastResponseHandler());
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed altering schema %s: %s", schema, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public List<Field> listColumns(VastTransaction transaction, String schema, String table, int pageSize)
            throws VastException
    {
        ArrayList<Field> fields = new ArrayList<>();
        String path = format("/%s/%s", schema, table);
        Function<VastResponse, List<Field>> fieldsExtractor = response -> {
            try {
                Schema result = arrowSchemaUtils.parseSchema(response.getBytes(), allocator);
                LOG.debug("%s: %s", path, result);
                return result.getFields();
            }
            catch (IOException e) {
                LOG.error(e, "failed to parse schema: %s", response);
                throw serializationException(format("Failed listing columns for %s/%s: code %s", schema, table, response.getStatus()), e);
            }
        };
        Consumer<List<Field>> resultConsumer = fields::addAll;
        pagedObjectsFetch(transaction, fieldsExtractor, resultConsumer, Requests.LIST_COLUMNS, path, pageSize);
        return fields;
    }

    public void queryData(
            VastTransaction transaction, VastTraceToken traceToken, String schemaName, String tableName, Schema schema, FlatBufferSerializer projections, FlatBufferSerializer predicate,
            Supplier<QueryDataResponseHandler> handlerSupplier, AtomicReference<URI> usedDataEndpoint,
            VastSplitContext split, VastSchedulingInfo schedulingInfo, List<URI> dataEndpoints, VastRetryConfig retryConfig, Optional<Integer> limit,
            Optional<String> bigCatalogSearchPath, QueryDataPagination pagination)
    {
        String path = format("/%s/%s", schemaName, tableName);
        byte[] body = serializeQueryDataRequestBody(path, schema, projections, predicate);
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.length)
                .build();
        // TODO: make split serialization look better
        headers.put("tabular-split", format("%s,%s,%s", split.getCurrentSplit(), split.getNumOfSplits(), split.getRowGroupsPerSubSplit()));
        headers.put("tabular-num-of-subsplits", Integer.toString(split.getNumOfSubSplits()));
        // TODO: drop the below headers (both from here and Debbie)
        headers.put("tabular-request-format", "string");
        headers.put("tabular-response-format", "string");
        headers.put("tabular-client-tag-trace-token", traceToken.toString());
        schedulingInfo.updateHeaders(headers);
        pagination.updateHeaders(headers);
        limit.ifPresent(value -> headers.put("tabular-limit-rows", Integer.toString(value)));
        bigCatalogSearchPath.ifPresent(value -> headers.put("tabular-bc-search-path", value));

        for (int i = 0; i <= retryConfig.getMaxRetries(); ++i) {
            // TODO: currently, we do a round-robin between configured endpoints - in the future, we should get them by querying Vast.
            URI dataEndpoint = dataEndpoints.get((split.getCurrentSplit() + i) % dataEndpoints.size());
            LOG.debug("QueryData(%s) %s (retry %d/%d) is sent to %s", traceToken, split, i, retryConfig.getMaxRetries(), dataEndpoint);
            Request request = new VastRequestBuilder(dataEndpoint, config, GET, path, Requests.QUERY_DATA.getRequestParam())
                    .addHeaders(headers)
                    .addHeader("tabular-retry-count", Integer.toString(i))
                    .setBody(Optional.of(body))
                    .build();
            usedDataEndpoint.set(dataEndpoint);
            // QueryData is an idempotent RPC (parser is re-created on retry, pagination is set only on success)
            VastResponse queryDataResponse = retryIOErrorsAndTimeouts(() -> httpClient.execute(request, handlerSupplier.get()));
            HttpStatus statusCode = HttpStatus.fromStatusCode(queryDataResponse.getStatus());
            if (statusCode == OK) {
                return;
            }
            if (statusCode == SERVICE_UNAVAILABLE) {
                LOG.warn("QueryData(%s) Service unavailable. Retrying", traceToken);
                if (i < retryConfig.getMaxRetries()) {
                    try {
                        Thread.sleep(retryConfig.getSleepDuration());
                    }
                    catch (InterruptedException e) {
                        throw toRuntime(serverException(format("QueryData(%s) retry sleep interrupted: %s", traceToken, i), e));
                    }
                }
            }
            else {
                throw toRuntime(VastExceptionFactory.checkResponseStatus(queryDataResponse, format("QueryData(%s) failed with status code: %s", traceToken, statusCode.code())).get());
            }
        }
        throw toRuntime(serverException(format("QueryData(%s) failed after %d retries", traceToken, retryConfig.getMaxRetries())));
    }

    public void insertRows(VastTransaction transaction, String schema, String table, VectorSchemaRoot root, URI dataEndpoint, Optional<Integer> maxRowsPerInsert)
            throws VastException
    {
        String path = format("/%s/%s", schema, table);
        for (byte[] body : splitter.split(root, maxRowsPerInsert.orElse(config.getMaxRowsPerInsert()))) {
            Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                    .withTransaction(transaction)
                    .withContentLength(body.length)
                    .build();

            Request request = new VastRequestBuilder(dataEndpoint, config, POST, path, Requests.INSERT_ROWS.getRequestParam())
                    .addHeaders(headers)
                    .setBody(Optional.of(body))
                    .build();
            VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
            Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed inserting rows to table %s: %s", path, response));
            if (abstractVastException.isPresent()) {
                throw abstractVastException.get();
            }
            // TODO: implement insert+update flow (for very wide RecordBatches)
        }
    }

    public void deleteRows(VastTransaction transaction, String schema, String table, VectorSchemaRoot root, URI dataEndpoint, Optional<Integer> maxRowsPerDelete)
            throws VastException
    {
        String path = format("/%s/%s", schema, table);
        for (byte[] body : splitter.split(root, maxRowsPerDelete.orElse(config.getMaxRowsPerDelete()))) {
            Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                    .withTransaction(transaction)
                    .withContentLength(body.length)
                    .build();

            Request request = new VastRequestBuilder(dataEndpoint, config, DELETE, path, Requests.DELETE_ROWS.getRequestParam())
                    .addHeaders(headers)
                    .setBody(Optional.of(body))
                    .build();
            // TODO: due to ORION-96056, DeleteRows is not an idempotent RPC
            VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
            Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed deleting rows from %s: %s", path, response));
            if (abstractVastException.isPresent()) {
                throw abstractVastException.get();
            }
        }
    }

    public void updateRows(VastTransaction transaction, String schema, String table, VectorSchemaRoot root, URI dataEndpoint, Optional<Integer> maxRowsPerUpdate)
            throws VastException
    {
        String path = format("/%s/%s", schema, table);
        for (byte[] body : splitter.split(root, maxRowsPerUpdate.orElse(config.getMaxRowsPerUpdate()))) {
            Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                    .withTransaction(transaction)
                    .withContentLength(body.length)
                    .build();

            Request request = new VastRequestBuilder(dataEndpoint, config, PUT, path, Requests.UPDATE_ROWS.getRequestParam())
                    .addHeaders(headers)
                    .setBody(Optional.of(body))
                    .build();
            // TODO: due to ORION-96056, UpdateRows is not an idempotent RPC
            VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
            Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed updating rows in %s: %s", path, response));
            if (abstractVastException.isPresent()) {
                throw abstractVastException.get();
            }
        }
    }

    public void createTable(VastTransaction transaction, CreateTableContext ctx)
            throws VastException
    {
        String path = BASE + ctx.getSchemaName() + BASE + ctx.getTableName();
        Optional<byte[]> body = schemaSerializer.apply(arrowSchemaUtils.fromCreateTableContext(ctx));
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.map(bytes -> bytes.length).orElse(0))
                .build();
        Request req = new VastRequestBuilder(config, POST, path, Requests.CREATE_TABLE.getRequestParam())
                .addHeaders(headers)
                .setBody(body)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed creating table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void dropTable(VastTransaction transaction, DropTableContext ctx)
            throws VastException
    {
        String path = BASE + ctx.getSchemaName() + BASE + ctx.getTableName();
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        Request req = new VastRequestBuilder(config, DELETE, path, Requests.DROP_TABLE.getRequestParam())
                .addHeaders(headers)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed dropping table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void alterTable(VastTransaction transaction, String schemaName, String tableName, AlterTableContext ctx)
            throws VastException
    {
        String path = BASE + schemaName + BASE + tableName;
        Optional<byte[]> body = Optional.ofNullable(arrowSchemaUtils.fromAlterTableContext(ctx));
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.map(bytes -> bytes.length).orElse(0))
                .build();
        Optional<Map<String, String>> of = ctx.getName().isPresent() ?
                Optional.of(ImmutableMap.of("tabular-new-table-name", ctx.getName().get())) :
                VastRequestBuilder.EMPTY_KV_PARAMS;
        Request req = new VastRequestBuilder(config, PUT, path, of, Requests.ALTER_TABLE.getRequestParam())
                .addHeaders(headers)
                .setBody(body)
                .build();
        VastResponse response = httpClient.execute(req, VastResponseHandler.createVastResponseHandler());
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed altering table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void alterColumn(VastTransaction transaction, String schemaName, String tableName, AlterColumnContext ctx)
            throws VastException
    {
        String path = BASE + schemaName + BASE + tableName;
        Optional<byte[]> body = Optional.ofNullable(arrowSchemaUtils.fromAlterColumnContext(ctx));
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.map(bytes -> bytes.length).orElse(0))
                .build();
        HashMap<String, String> urlParams = new HashMap<>();
        urlParams.put("tabular-column-name", ctx.getName());
        ctx.getNewName().ifPresent(newName -> urlParams.put("tabular-new-column-name", newName));
        Request req = new VastRequestBuilder(config, PUT, path, Optional.of(urlParams), Requests.ALTER_COLUMNS.getRequestParam())
                .addHeaders(headers)
                .setBody(body)
                .build();
        VastResponse response = httpClient.execute(req, VastResponseHandler.createVastResponseHandler());
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed altering column for table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void dropColumn(VastTransaction transaction, TableColumnLifecycleContext ctx)
            throws VastException
    {
        changeColumnLifeCycle(transaction, ctx, DELETE, Requests.DROP_COLUMN, "Drop");
    }

    public void addColumn(VastTransaction transaction, TableColumnLifecycleContext ctx)
            throws VastException
    {
        changeColumnLifeCycle(transaction, ctx, POST, Requests.ADD_COLUMN, "Add");
    }

    private void changeColumnLifeCycle(VastTransaction transaction, TableColumnLifecycleContext ctx, HttpMethodName method, Requests requestName, String operation)
            throws VastException
    {
        LOG.debug("changeColumnLifeCycle: %s: %s", method, ctx);
        Optional<byte[]> body = schemaSerializer.apply(arrowSchemaUtils.fromChangeColumnLifeCycleContext(ctx));
        String path = BASE + ctx.getSchemaName() + BASE + ctx.getTableName();
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.map(bytes -> bytes.length).orElse(0))
                .build();
        Request req = new VastRequestBuilder(config, method, path, requestName.getRequestParam())
                .addHeaders(headers)
                .setBody(body)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response,
                format("Failed executing %s for column %s of table %s: %s", operation, ctx.getColumnName(), path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    private byte[] serializeQueryDataRequestBody(String tablePath, Schema schema, FlatBufferSerializer projections, FlatBufferSerializer predicate)
    {
        LOG.debug("Serializing query-data request for table %s: %s", tablePath, schema);

        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        int nameOffset = builder.createString(tablePath);
        int schemaOffset = schema.getSchema(builder); // serialize the schema
        int filterOffset = predicate.serialize(builder);

        Source.startSource(builder);
        Source.addName(builder, nameOffset);
        Source.addSchema(builder, schemaOffset);
        Source.addFilter(builder, filterOffset);
        int sourceOffset = Source.endSource(builder);
        int childRel = Relation.createRelation(builder, RelationImpl.Source, sourceOffset);

        int expressionsOffset = projections.serialize(builder);
        Project.startProject(builder);
        Project.addRel(builder, childRel);
        Project.addExpressions(builder, expressionsOffset);
        int projectOffset = Project.endProject(builder);

        int relationOffset = Relation.createRelation(builder, RelationImpl.Project, projectOffset);
        builder.finish(relationOffset);
        byte[] result = builder.sizedByteArray(); // TODO: don't copy the data
        if (LOG.isDebugEnabled()) {
            LOG.debug("QueryData request body for table %s: %s", tablePath, Hex.encodeHexString(result));
        }
        return result;
    }

    public VastResponse importData(VastTransaction transaction, VastTraceToken traceToken, ImportDataContext ctx, Consumer<InputStream> importDataResponseConsumer, URI dataEndpoint)
    {
        LOG.info("ImportData(%s): sending request to VAST URI: %s", traceToken, dataEndpoint);
        String path = ctx.getDest();
        Optional<byte[]> body = Optional.of(byteBufferToArray(arrowSchemaUtils.newImportDataRequest(ctx, allocator)));
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withTraceToken(traceToken)
                .withContentLength(body.map(bytes -> bytes.length).orElse(0))
                .withCaseSensitive(false)
                .build();
        Request req = new VastRequestBuilder(dataEndpoint, config, POST, path, Requests.IMPORT_DATA.getRequestParam())
                .addHeaders(headers)
                .setBody(body)
                .build();
        return retryConnectionErrors(() ->
                httpClient.execute(req, VastResponseHandler.createVastResponseHandlerForCustomInputStreamConsumption(importDataResponseConsumer)));
    }

    private byte[] byteBufferToArray(ByteBuffer byteBuffer)
    {
        byte[] br = new byte[byteBuffer.remaining()];
        byteBuffer.get(br);
        return br;
    }

    public VastResponse startTransaction(StartTransactionContext ctx)
    {
        Optional<byte[]> emptyBody = Optional.of(new byte[0]); //tempfix for POST without body auth issue
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withReadOnlyTransaction(ctx.isReadOnly())
                .build();
        Request req = new VastRequestBuilder(config, POST, BASE, Requests.START_TRANSACTION.getRequestParam())
                .addHeaders(headers)
                .setBody(emptyBody)
                .build();
        return retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public VastResponse rollbackTransaction(VastTransaction transaction)
    {
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        Request req = new VastRequestBuilder(config, DELETE, BASE, Requests.ROLLBACK_TRANSACTION.getRequestParam())
                .addHeaders(headers)
                .build();
        return retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public VastResponse commitTransaction(VastTransaction transaction)
    {
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        Request req = new VastRequestBuilder(config, PUT, BASE, Requests.COMMIT_TRANSACTION.getRequestParam())
                .addHeaders(headers)
                .build();
        return retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public VastSchedulingInfo getSchedulingInfo(VastTransaction transaction, VastTraceToken traceToken, String schemaName, String tableName)
    {
        String path = format("/%s/%s", schemaName, tableName);
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withTraceToken(traceToken)
                .build();
        Request req = new VastRequestBuilder(config, GET, path, Requests.GET_SCHEDULING_INFO.getRequestParam())
                .addHeaders(headers)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        return VastSchedulingInfo.create(response);
    }

    public VastStatistics getTableStats(VastTransaction transaction, String schemaName, String tableName)
            throws VastException
    {
        String path = BASE + schemaName + BASE + tableName;
        AtomicReference<VastStatistics> results = new AtomicReference<>();
        singleObjectFetch(transaction, VastClient::parseGetTableStats, results::set, Requests.GET_TABLE_STATS, path, tableName);
        LOG.info("Got stats for table %s: %s", path, results);
        return results.get();
    }

    private <T> T retryConnectionErrors(Provider<T> call)
    {
        return retryIfNeeded(call, VastClient::ignoreConnectionError);
    }

    // Can be used ONLY for idempotent RPCs! (since an IOException can happen during/after RPC execution)
    private <T> T retryIOErrorsAndTimeouts(Provider<T> call)
    {
        return retryIfNeeded(call, VastClient::ignoreIOErrorAndTimeouts);
    }

    private <T> T retryIfNeeded(Provider<T> call, Consumer<RuntimeException> ignore)
    {
        for (int i = 0; i <= config.getRetryMaxCount(); ++i) {
            try {
                return call.get();
            }
            catch (RuntimeException e) {
                ignore.accept(e);
                try {
                    Thread.sleep(config.getRetrySleepDuration());
                }
                catch (InterruptedException ie) {
                    throw toRuntime(ie);
                }
            }
        }
        throw toRuntime(serverException(format("Request failed after %d retries", config.getRetryMaxCount())));
    }

    private static void ignoreConnectionError(RuntimeException e)
    {
        if (e instanceof UncheckedIOException && (e.getCause() instanceof ConnectException || e.getCause() instanceof NoRouteToHostException)) {
            // May happen during HA
            LOG.warn(e, "retrying due to connection failure");
            return;
        }
        // Specific case for ORION-106029 - retry without breaking idempotency
        else {
            Throwable cause = e.getCause();
            if (cause != null) {
                while (!(cause instanceof EOFException) && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                if (cause instanceof EOFException && cause.getCause() == null && cause.getMessage().contains(",to=0/")) {
                    LOG.warn(e, "retrying due to connection closed without sending the request");
                    return;
                }
            }
        }
        throw e;
    }

    private static void ignoreIOErrorAndTimeouts(RuntimeException e)
    {
        if (e instanceof UncheckedIOException && e.getCause() != null) {
            // May happen during HA
            LOG.warn(e, "retrying due to I/O error");
            return;
        }
        if (e.getCause() instanceof TimeoutException) {
            // May happen during HA
            LOG.warn(e, "retrying due to timeout error");
            return;
        }
        ignoreConnectionError(e);
    }
}
