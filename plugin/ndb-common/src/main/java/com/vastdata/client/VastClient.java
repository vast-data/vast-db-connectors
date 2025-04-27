/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Provider;
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
import com.vastdata.client.schema.DropViewContext;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.schema.VastPayloadSerializer;
import com.vastdata.client.schema.VastViewMetadata;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.apache.commons.codec.binary.Hex;
import vast_flatbuf.tabular.GetTableStatsResponse;
import vast_flatbuf.tabular.ListSchemasResponse;
import vast_flatbuf.tabular.ListTablesResponse;
import vast_flatbuf.tabular.ListViewsResponse;
import vast_flatbuf.tabular.ObjectDetails;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

    public static final String AUDIT_LOG_BUCKET_NAME = "vast-audit-log-bucket"; // special bucket name for audit log support
    public static final String BIG_CATALOG_BUCKET_NAME = "vast-big-catalog-bucket"; // special bucket name for BigCatalog support
    public static final String SYSTEM_SCHEMA_NAME = "system_schema";
    public static final String INFORMATION_SCHEMA_NAME = "information_schema";

    public static final Set<String> INTERNAL_BUCKETS_NO_VIEW_OPERATIONS = Stream.of(
            AUDIT_LOG_BUCKET_NAME,
            BIG_CATALOG_BUCKET_NAME
    ).collect(Collectors.toSet());

    private static final Set<String> SPECIAL_SCHEMAS = Stream.of(
            SYSTEM_SCHEMA_NAME,
            INFORMATION_SCHEMA_NAME
    ).collect(Collectors.toSet());

    private static final RootAllocator allocator = new RootAllocator();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final ArrowSchemaUtils arrowSchemaUtils;
    private final VastConfig config;
    private final HttpClient httpClient;
    private final VastDependenciesFactory dependenciesFactory;
    private final VastPayloadSerializer<Schema> schemaSerializer;
    private final VastPayloadSerializer<VectorSchemaRoot> recordBatchSerializer;

    private final RecordBatchSplitter splitter;

    private final Schema INSERTED_ROW_IDS_SCHEMA = new Schema(ImmutableList.of(new Field("$row_id", new FieldType(true, new ArrowType.Int(64, false), null), ImmutableList.of())));

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

    private static ListViewsResponse parseListViewsResponse(final VastResponse response)
    {
        try {
            return ListViewsResponse.getRootAsListViewsResponse(response.getByteBuffer());
        }
        catch (final Throwable error) {
            LOG.error(error, "%s: failed to parse ListViewsResponse: %s", response, Hex.encodeHexString(response.getByteBuffer()));
            throw error;
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

    private static final Function<ListViewsResponse, Stream<String>> streamListViewResponse = r -> IntStream
            .range(0, r.viewsLength())
            .mapToObj(r::views)
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
        this(httpClient, config, dependenciesFactory, new ArrowSchemaUtils());
    }

    @VisibleForTesting
    protected VastClient(HttpClient httpClient, VastConfig config, VastDependenciesFactory dependenciesFactory,
            ArrowSchemaUtils arrowSchemaUtils)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.dependenciesFactory = dependenciesFactory;
        this.schemaSerializer = VastPayloadSerializer.getInstanceForSchema();
        this.recordBatchSerializer = VastPayloadSerializer.getInstanceForRecordBatch();
        this.splitter = new RecordBatchSplitter(config.getMaxRequestBodySize(), recordBatchSerializer);
        this.arrowSchemaUtils = arrowSchemaUtils;
    }

    public List<String> listBuckets(boolean includeHidden)
            throws VastIOException
    {
        LOG.debug("Listing buckets");
        Request request = new VastRequestBuilder(config, GET, BASE).build();
        VastResponse response = retryIOErrorsAndTimeouts(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandlerForListBuckets()));
        try {
            ImmutableList.Builder<String> result = ImmutableList.builder();
            result.add(OBJECT_MAPPER.readValue(response.getBytes(), String[].class));
            if (includeHidden) {
                result.add(AUDIT_LOG_BUCKET_NAME);
                result.add(BIG_CATALOG_BUCKET_NAME);
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

        Request request = new VastRequestBuilder(config, GET, path, ImmutableMap.of(requestName.getRequestParam(), ""))
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
            throw serverException(format("Failed fetching object %s: code %s; message: %s", nameToMatch, responseStatus, response.getErrorMessage()));
        }
        resultConsumer.accept(resultProvider.apply(response));
    }

    private <T> void pagedObjectsFetch(VastTransaction transaction,
            Function<VastResponse, T> resultProvider,
            Consumer<T> resultConsumer,
            Requests requestName,
            String path,
            int pageSize, Map<String, String> extraQueryParams)
            throws VastServerException, VastUserException
    {
        boolean fetchNextPage = true;
        Long nextKey = 0L;
        while (fetchNextPage) {
            VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory()
                    .withNextKey(nextKey)
                    .withMaxKeys(pageSize);

            if (transaction != null) {
                headersFactory = headersFactory.withTransaction(transaction);
            }
            final Multimap<String, String> headers = headersFactory.build();

            Request request = new VastRequestBuilder(config, GET, path, getQueryParamsMap(extraQueryParams, requestName))
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
                throw serverException(format("Failed to execute request %s for path %s: code %s; message: %s", requestName.name(), path, responseStatus, response.getErrorMessage()));
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
                    .map(obj -> format("%s/%s", parentPath, obj.name()))
                    .forEachOrdered(searchStack::add);
            try {
                pagedObjectsFetch(transaction, VastClient::parseListSchemasResponse, resultConsumer, Requests.LIST_SCHEMA, BASE + parentPath, pageSize, Collections.emptyMap());
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
        if (SPECIAL_SCHEMAS.contains(schemaName)) {
            return true;
        }

        ParsedURL schemaUrl = ParsedURL.of(schemaName);
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));

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
        pagedObjectsFetch(transaction, VastClient::parseListTablesResponse, resultConsumer, Requests.LIST_TABLES, path, pageSize, Collections.emptyMap());
        return results.stream();
    }

    public Stream<String> listViews(final VastTransaction transaction, final String schema, final int pageSize)
            throws VastServerException, VastUserException
    {
        if (SPECIAL_SCHEMAS.contains(schema) || INTERNAL_BUCKETS_NO_VIEW_OPERATIONS.stream().anyMatch(bucket -> schema.startsWith(bucket + "/"))) {
            LOG.debug("listViews(%s, ...) filtered out because %s is internal: %s or %s", transaction, schema, SPECIAL_SCHEMAS, INTERNAL_BUCKETS_NO_VIEW_OPERATIONS);
            return Stream.empty();
        }
        final String path = BASE + schema;
        final LinkedBlockingQueue<String> results = new LinkedBlockingQueue<>();
        final Consumer<ListViewsResponse> resultConsumer = r -> streamListViewResponse.apply(r).forEach(results::add);
        pagedObjectsFetch(transaction, VastClient::parseListViewsResponse, resultConsumer, Requests.LIST_VIEWS, path, pageSize, Collections.emptyMap());
        return results.stream();
    }

    public Optional<String> getVastTableHandleId(VastTransaction transaction, String schemaName, String tableName)
            throws VastServerException, VastUserException
    {
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(!Strings.isNullOrEmpty(tableName), "tableName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));
        String path = BASE + schemaName;
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Consumer<ListTablesResponse> responseConsumer = r -> streamListTablesResponseHandle.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListTablesResponse, responseConsumer, Requests.LIST_TABLES, path, tableName);
        LOG.debug("Matching table handle: %s for path: %s", results, path);
        VerifyParam.verify(results.size() <= 1,
                format("Vast Table handle must not contain multiple elements: %s", results));
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void s3PutObj(String keyName, String bucketName, String statsStr) {
        try {
            retryAwsSdkErrors(() -> getPutObjectResult(keyName, bucketName, statsStr));
            LOG.info("Uploading object %s to Vast succeeded", keyName);
        } catch (RuntimeException e) {
            LOG.error(e, "Uploading object %s to Vast failed", keyName);
            throw e;
        }
    }

    public PutObjectResult getPutObjectResult(String keyName, String bucketName, String data)
    {
        final AmazonS3 s3 = getAmazonS3Client();
        return s3.putObject(bucketName, keyName, data);
    }

    public Optional<String> s3GetObj(String keyName, String bucketName) {
        try {
            String statsStr = retryAwsSdkErrors(() -> getObjectAsString(keyName, bucketName));
            LOG.info("Downloading object %s from Vast succeeded: %s", keyName, statsStr);
            return Optional.of(statsStr);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                LOG.warn("Couldn't find statistics file %s in bucket %s", keyName, bucketName);
                return Optional.empty();
            } else {
                LOG.warn(e, "S3 GET failed for object %s, bucket: %s", keyName, bucketName);
                return null;
            }
        } catch (Exception e) {
            LOG.warn(e, "Failed to get object %s, bucket: %s", keyName, bucketName);
            return null;
        }
    }

    public String getObjectAsString(String keyName, String bucketName)
    {
        final AmazonS3 s3 = getAmazonS3Client();
        return s3.getObjectAsString(bucketName, keyName);
    }

    private AmazonS3 getAmazonS3Client()
    {
        AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey());
        AwsClientBuilder.EndpointConfiguration ec = new AwsClientBuilder.EndpointConfiguration(config.getEndpoint().toString(), config.getRegion());
        return AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(Boolean.TRUE)
                .withEndpointConfiguration(ec)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    public boolean tableExists(VastTransaction transaction, String schemaName, String tableName)
            throws VastServerException, VastUserException
    {
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(!Strings.isNullOrEmpty(tableName), "tableName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));
        String path = BASE + schemaName;

        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Consumer<ListTablesResponse> responseConsumer = r -> streamListTablesResponse.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListTablesResponse, responseConsumer, Requests.LIST_TABLES, path, tableName);
        LOG.debug("matching tables: %s", results);
        return results.equals(ImmutableList.of(tableName));
    }

    public boolean viewExists(final VastTransaction transaction, final String schemaName, final String viewName)
            throws VastServerException, VastUserException
    {
        LOG.debug("starting viewExists(%s, %s, %s)", transaction, schemaName, viewName);
        if (SPECIAL_SCHEMAS.contains(schemaName) || INTERNAL_BUCKETS_NO_VIEW_OPERATIONS.stream().anyMatch(bucket -> schemaName.startsWith(bucket + "/"))) {
            LOG.debug("viewExists(%s, ...) filtered out because %s is internal: %s or %s", transaction, schemaName, SPECIAL_SCHEMAS, INTERNAL_BUCKETS_NO_VIEW_OPERATIONS);
            return false;
        }
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "schemaName is null or empty");
        VerifyParam.verify(!Strings.isNullOrEmpty(viewName), "viewName is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));
        final String path = BASE + schemaName;

        final List<String> results = Collections.synchronizedList(new ArrayList<>());
        final Consumer<ListViewsResponse> responseConsumer = r -> streamListViewResponse.apply(r).forEach(results::add);
        singleObjectFetch(transaction, VastClient::parseListViewsResponse, responseConsumer, Requests.LIST_VIEWS, path, viewName);
        LOG.debug("matching views: %s", results);
        return results.equals(ImmutableList.of(viewName));
    }

    public void createSchema(VastTransaction transaction, String schemaName, String serializedProperties)
            throws VastException
    {
        LOG.info("Creating schema %s", schemaName);
        VerifyParam.verify(!Strings.isNullOrEmpty(schemaName), "Schema name is null or empty");
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory();
        byte[] body = arrowSchemaUtils.serializeCreateSchemaBody(serializedProperties);
        Multimap<String, String> headers = headersFactory
                .withTransaction(transaction)
                .withContentLength(body.length)
                .build();
        Request request = new VastRequestBuilder(config, POST, BASE + schemaName, ImmutableMap.of(Requests.CREATE_SCHEMA.getRequestParam(), ""))
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
        VerifyParam.verify(dependenciesFactory.getSchemaNameValidator().test(schemaName), format("schema name is invalid: \"%s\"", schemaName));
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory();
        Multimap<String, String> headers = headersFactory
                .withTransaction(transaction)
                .build();
        Request request = new VastRequestBuilder(config, DELETE, BASE + schemaName, VastRequestBuilder.EMPTY_KV_PARAMS, ImmutableMap.of(Requests.DROP_SCHEMA.getRequestParam(), ""))
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
        Request request = new VastRequestBuilder(config, PUT, BASE + schema, urlParams, ImmutableMap.of(Requests.ALTER_SCHEMA.getRequestParam(), ""))
                .setBody(Optional.of(body))
                .addHeaders(headers)
                .build();
        VastResponse response = httpClient.execute(request, VastResponseHandler.createVastResponseHandler());
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed altering schema %s: %s", schema, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public List<Field> listColumns(VastTransaction transaction, String schema, String table, int pageSize, Map<String, String> extraQueryParams)
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
        pagedObjectsFetch(transaction, fieldsExtractor, resultConsumer, Requests.LIST_COLUMNS, path, pageSize, extraQueryParams);
        return fields;
    }

    public void queryData(
            VastTransaction transaction, VastTraceToken traceToken, String schemaName, String tableName, Schema schema,
            FlatBufferSerializer projections, FlatBufferSerializer predicate,
            Supplier<QueryDataResponseHandler> handlerSupplier, AtomicReference<URI> usedDataEndpoint,
            VastSplitContext split, VastSchedulingInfo schedulingInfo, List<URI> dataEndpoints,
            VastRetryConfig retryConfig, Optional<Integer> limit,
            Optional<String> bigCatalogSearchPath, QueryDataPagination pagination,
            boolean enableSortedProjections, Map<String, String> extraQueryParams)
    {
        String path = format("/%s/%s", schemaName, tableName);
        LOG.debug("Serializing query-data request for table %s: %s", path, schema);
        byte[] body = arrowSchemaUtils.serializeQueryDataRequestBody(path, schema, projections, predicate);
        if (LOG.isDebugEnabled()) {
            LOG.debug("QueryData request body for table %s: %s", path, Hex.encodeHexString(body));
        }
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory()
                .withContentLength(body.length);

        if (transaction != null) {
            headersFactory = headersFactory.withTransaction(transaction);
        }
        final Multimap<String, String> headers = headersFactory.build();

        // TODO: make split serialization look better
        headers.put("tabular-split", format("%s,%s,%s", split.getCurrentSplit(), split.getNumOfSplits(), split.getRowGroupsPerSubSplit()));
        headers.put("tabular-num-of-subsplits", Integer.toString(split.getNumOfSubSplits()));
        // TODO: drop the below headers (both from here and Debbie)
        headers.put("tabular-request-format", "string");
        headers.put("tabular-response-format", "string");
        headers.put("tabular-client-tag-trace-token", traceToken.toString());
        if (enableSortedProjections) {
            headers.put("tabular-enable-sorted-projections", "true");
        }
        if (schedulingInfo != null) {
            schedulingInfo.updateHeaders(headers);
        }
        pagination.updateHeaders(headers);
        limit.ifPresent(value -> headers.put("tabular-limit-rows", Integer.toString(value)));
        bigCatalogSearchPath.ifPresent(value -> headers.put("tabular-bc-search-path", value));

        AtomicInteger currAttempt = new AtomicInteger(0);
        HashMap<String, String> reqParamsMap = getQueryParamsMap(extraQueryParams, Requests.QUERY_DATA);
        for (int i = 0; i <= retryConfig.getMaxRetries(); ++i) {
            // TODO: currently, we do a round-robin between configured endpoints - in the future, we should get them by querying Vast.
            //            usedDataEndpoint.set(dataEndpoint);
            Supplier<Request> requestSupplier = () -> {
                int attemptIndex = currAttempt.getAndIncrement();
                URI uri = dataEndpoints.get(getEndpointIndex(tableName, split, attemptIndex, dataEndpoints.size()));
                LOG.debug("QueryData(%s) %s (retry %d/%d) is sent to %s", traceToken, split, attemptIndex, retryConfig.getMaxRetries(), uri);

                return new VastRequestBuilder(uri, config, GET, path, reqParamsMap)
                        .addHeaders(headers)
                        .addHeader("tabular-retry-count", Integer.toString(attemptIndex))
                        .setBody(Optional.of(body))
                        .build();
            };
            // QueryData is an idempotent RPC (parser is re-created on retry, pagination is set only on success)
            VastResponse queryDataResponse = retryIfNeeded(() -> httpClient.execute(requestSupplier.get(), handlerSupplier.get()), ignored -> {
                LOG.debug(ignored, "Retrying `queryData`");
            });
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

    private static HashMap<String, String> getQueryParamsMap(Map<String, String> extraQueryParams, Requests request)
    {
        HashMap<String, String> reqParamsMap = new HashMap<>();
        reqParamsMap.put(request.getRequestParam(), "");
        reqParamsMap.putAll(extraQueryParams);
        return reqParamsMap;
    }

    @VisibleForTesting
    protected static int getEndpointIndex(String tableName, VastSplitContext split, int i, int size)
    {
        return (split.getCurrentSplit() + i + (tableName.hashCode() & Integer.MAX_VALUE)) % size;
    }

    private VectorSchemaRoot sendSomeRows(final VastTransaction transaction,
                                  final URI endpoint,
                                  final Requests requestType,
                                  final String path,
                                  final byte[] body,
                                  final boolean readResult)
            throws VastException
    {
        VastRequestHeadersBuilder headersFactory = dependenciesFactory.getHeadersFactory()
                .withContentLength(body.length);

        if (transaction != null) {
            headersFactory = headersFactory.withTransaction(transaction);
        }
        final Multimap<String, String> headers = headersFactory.build();

        final Request request = new VastRequestBuilder(endpoint, config, POST, path, ImmutableMap.of(requestType.getRequestParam(), "") )
                .addHeaders(headers)
                .setBody(Optional.of(body))
                .build();
        final VastResponse response = retryConnectionErrors(() -> httpClient.execute(request, VastResponseHandler.createVastResponseHandler()));
        final Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed inserting rows to table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }

        if (!readResult) {
            return null;
        }
        else {
            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(response.getBytes()), allocator)) {
                try {
                    if (!reader.loadNextBatch()) {
                        throw toRuntime(serverException("Expecting a RecordBatch in response containing inserted row ids"));
                    }
                    final VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    final VectorUnloader unloader = new VectorUnloader(root);
                    final VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
                    final VectorLoader loader = new VectorLoader(copy);
                    loader.load(unloader.getRecordBatch());

                    if (reader.loadNextBatch()) {
                        throw toRuntime(serverException("Expecting exactly one batch. got more"));
                    }
                    return copy;
                } catch (IOException e) {
                    throw new VastIOException("Failed to read response", e);
                }
            } catch (IOException e) {
                throw toRuntime("Failed reading insert response", e);
            }
        }
    }

    public void insertRows(VastTransaction transaction, String schema, String table, VectorSchemaRoot root, URI dataEndpoint, Optional<Integer> maxRowsPerInsert)
            throws VastException
    {
        final String path = format("/%s/%s", schema, table);
        for (byte[] body : splitter.split(root, maxRowsPerInsert.orElse(config.getMaxRowsPerInsert()))) {
            sendSomeRows(transaction, dataEndpoint, Requests.INSERT_ROWS, path, body, false);
        }
    }

    public VectorSchemaRoot insertRows(VastTransaction transaction, String schema, String table, VectorSchemaRoot root, URI dataEndpoint, Optional<Integer> maxRowsPerInsert, final boolean readResult)
            throws VastException
    {
        String path = format("/%s/%s", schema, table);

        final VectorSchemaRoot allRowIds = VectorSchemaRoot.create(INSERTED_ROW_IDS_SCHEMA, allocator);

        for (byte[] body : splitter.split(root, maxRowsPerInsert.orElse(config.getMaxRowsPerInsert()))) {
            final VectorSchemaRoot rowIds = sendSomeRows(transaction, dataEndpoint, Requests.INSERT_ROWS, path, body, readResult);
            VectorSchemaRootAppender.append(allRowIds, rowIds);
        }
        return allRowIds;
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

            Request request = new VastRequestBuilder(dataEndpoint, config, DELETE, path, ImmutableMap.of(Requests.DELETE_ROWS.getRequestParam(), ""))
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
        List<byte[]> split = splitter.split(root, maxRowsPerUpdate.orElse(config.getMaxRowsPerUpdate()));
        for (byte[] body : split) {
            Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                    .withTransaction(transaction)
                    .withContentLength(body.length)
                    .build();

            Request request = new VastRequestBuilder(dataEndpoint, config, PUT, path, ImmutableMap.of(Requests.UPDATE_ROWS.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(config, POST, path, ImmutableMap.of(Requests.CREATE_TABLE.getRequestParam(), ""))
                .addHeaders(headers)
                .setBody(body)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed creating table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void createView(final VastTransaction transaction, final VastViewMetadata ctx)
            throws VastException
    {
        final String path = BASE + ctx.getSchemaName() + BASE + ctx.getViewName();
        final Optional<byte[]> metadata = recordBatchSerializer.apply(ctx.metadata());
        final Optional<byte[]> schema = schemaSerializer.apply(ctx.schema());
        final byte[] body = arrowSchemaUtils.serializeCreateViewRequestBody(schema.get(), metadata.get());
        final Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withContentLength(body.length)
                .build();
        final Request req = new VastRequestBuilder(config, POST, path, ImmutableMap.of(Requests.CREATE_VIEW.getRequestParam(), ""))
                .addHeaders(headers)
                .setBody(Optional.of(body))
                .build();
        final VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        final Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed creating view %s: %s", path, response));
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
        Request req = new VastRequestBuilder(config, DELETE, path, ImmutableMap.of(Requests.DROP_TABLE.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed dropping table %s: %s", path, response));
        if (abstractVastException.isPresent()) {
            throw abstractVastException.get();
        }
    }

    public void dropView(final VastTransaction transaction, final DropViewContext ctx)
            throws VastException
    {
        final String path = BASE + ctx.getSchemaName() + BASE + ctx.getViewName();
        final Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        final Request req = new VastRequestBuilder(config, DELETE, path, ImmutableMap.of(Requests.DROP_VIEW.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        final VastResponse response = retryConnectionErrors(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
        final Optional<VastException> abstractVastException = VastExceptionFactory.checkResponseStatus(response, format("Failed dropping view %s: %s", path, response));
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
        Request req = new VastRequestBuilder(config, PUT, path, of, ImmutableMap.of(Requests.ALTER_TABLE.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(config, PUT, path, Optional.of(urlParams), ImmutableMap.of(Requests.ALTER_COLUMNS.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(config, method, path, ImmutableMap.of(requestName.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(dataEndpoint, config, POST, path, ImmutableMap.of(Requests.IMPORT_DATA.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(config, POST, BASE, ImmutableMap.of(Requests.START_TRANSACTION.getRequestParam(), ""))
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
        Request req = new VastRequestBuilder(config, DELETE, BASE, ImmutableMap.of(Requests.ROLLBACK_TRANSACTION.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        return retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public VastResponse commitTransaction(VastTransaction transaction)
    {
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        Request req = new VastRequestBuilder(config, PUT, BASE, ImmutableMap.of(Requests.COMMIT_TRANSACTION.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        // There is always a possibility that sending a response will fail (e.g. due to HA) after the commit is applied
        // successfully on VAST. Therefore, a failed query doesn't imply that the table's data wasn't modified.
        // In order to clean-up uncommitted transactions (see https://vastdata.atlassian.net/browse/ORION-120144)
        // we retry commit RPC even on I/O errors, with the following scenarios:
        // 1. if the transaction wasn't committed, the retry will succeed (and clean-up the transaction).
        // 2. if the transaction was committed, the retry will fail - and the user will have to inspect the data to understand what happened.
        return retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public void transactionKeepAlive(VastTransaction transaction)
    {
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .build();
        Request req = new VastRequestBuilder(config, GET, BASE, ImmutableMap.of(Requests.GET_TRANSACTION.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
    }

    public VastSchedulingInfo getSchedulingInfo(VastTransaction transaction, VastTraceToken traceToken, String schemaName, String tableName)
    {
        String path = format("/%s/%s", schemaName, tableName);
        Multimap<String, String> headers = dependenciesFactory.getHeadersFactory()
                .withTransaction(transaction)
                .withTraceToken(traceToken)
                .build();
        Request req = new VastRequestBuilder(config, GET, path, ImmutableMap.of(Requests.GET_SCHEDULING_INFO.getRequestParam(), ""))
                .addHeaders(headers)
                .build();
        VastResponse response = retryIOErrorsAndTimeouts(() -> httpClient.execute(req, VastResponseHandler.createVastResponseHandler()));
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

    private <T> T retryAwsSdkErrors(Provider<T> call)
    {
        return retryIfNeeded(call, VastClient::ignoreAwsSdkError);
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
        RuntimeException lastException = null;
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
                lastException = e;
            }
        }
        String message = format("Request failed after %d retries: %s", config.getRetryMaxCount(), lastException);
        LOG.error(lastException, message);
        throw toRuntime(serverException(message, lastException));
    }

    private static void ignoreAwsSdkError(RuntimeException e)
    {
        if (e instanceof SdkClientException && e.getCause() != null) {
            Throwable cause = e.getCause();
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof ConnectException || cause instanceof UnknownHostException) {
                LOG.warn(e, "retrying due to AWS SDK connection failure");
                return;
            }
        }
        throw e;
    }

    private static void ignoreConnectionError(RuntimeException e)
    {
        if (e instanceof UncheckedIOException && (e.getCause() instanceof ConnectException || e.getCause() instanceof NoRouteToHostException)) {
            // May happen during HA
            LOG.warn(e, "retrying due to connection failure");
            return;
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
