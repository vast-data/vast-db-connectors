/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockUtils;
import com.vastdata.mockserver.VastMockS3Server;
import com.vastdata.mockserver.VastRootHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.mockito.Mock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static com.amazonaws.http.HttpMethodName.GET;
import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET;
import static com.vastdata.client.VastClientForTests.RETRY_MAX_COUNT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestVastClient
{
    private static VastMockS3Server mockServer;
    private static final VastRootHandler handler = new VastRootHandler();
    private int testPort;
    @Mock VastTransaction mockTransactionHandle;
    @Mock HttpClient mockHttpClient;

    @BeforeClass
    public void startMockServer()
            throws IOException
    {
        mockServer = new VastMockS3Server(0, handler);
        testPort = mockServer.start();
    }

    @AfterClass
    public void stopServer()
    {
        if (Objects.nonNull(mockServer)) {
            mockServer.close();
        }
    }

    @BeforeMethod
    public void clearMockServer()
    {
        handler.clearSchema();
        initMocks(this);
        when(mockTransactionHandle.getId()).thenReturn(Long.parseUnsignedLong("514026084031791104"));
    }

    @Test(enabled = false)
    public void testSignatureListBuckets()
    {
        /*
         * GET / HTTP/1.1
         * Host: localhost:9090
         * Accept-Encoding: identity
         * User-Agent: Boto3/1.9.0 Python/3.6.8 Linux/3.10.0-1160.el7.x86_64 Botocore/1.12.158
         * X-Amz-Date: 20220314T161610Z
         * X-Amz-Content-SHA256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
         * Authorization: AWS4-HMAC-SHA256 Credential=0bfefyXxRzyKCRSqOknW/20220314/vast/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=dddb65c751a5ed3fda8be4ab7a319376a49dcfbd837e4d2434f5f673ddb89eef
         *
         * HTTP/1.1 200 OK
         * x-amz-id-2: a00100000001
         * x-amz-request-id: a00100000001
         * Date: Mon, 14 Mar 2022 16:16:10 GMT
         * Content-Type: application/xml
         * Transfer-Encoding: chunked
         * Server: vast
         *
         * d9
         * <?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>100</ID><DisplayName>vast-user</DisplayName></Owner><Buckets></Buckets></ListAllMyBucketsResult>
         * 0
         */
        VastConfig config = new VastConfig()
                .setEndpoint(URI.create("http://localhost:9090"))
                .setRegion("vast")
                .setAccessKeyId("0bfefyXxRzyKCRSqOknW")
                .setSecretAccessKey("WHeraF+RiHB/E3/YPa0bIfngGw31vL/B8zebwpBb");

        Request request = new VastRequestBuilder(config, GET, "/")
                .setDate(new Date(1647274570000L)) // (aka 20220314T161610Z)
                .build();
        Multimap<String, String> headers = request.getHeaders();
        assertThat(headers.get("x-amz-content-sha256")).isEqualTo(ImmutableList.of("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
        assertThat(headers.get("X-Amz-Date")).isEqualTo(ImmutableList.of("20220314T161610Z"));
        assertThat(headers.get("Host")).isEqualTo(ImmutableList.of("localhost:9090"));
        assertThat(headers.get("Authorization")).isEqualTo(ImmutableList.of("AWS4-HMAC-SHA256 Credential=0bfefyXxRzyKCRSqOknW/20220314/vast/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=dddb65c751a5ed3fda8be4ab7a319376a49dcfbd837e4d2434f5f673ddb89eef"));
    }

    @Test(enabled = false)
    public void testSignatureListSchemas()
    {
        /*
         * connector.name=vast
         * endpoint=http://localhost:9090
         * access_key_id=pIX3SzyuQVmdrIVZnyy0
         * secret_access_key=5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps
         *
         * GET /bucket-for-tabular-api?schema HTTP/1.1
         * Host: localhost:9090
         * user-agent: VastData Tabular API 1.0 - 2022 (c)
         * Accept-Encoding: gzip, deflate
         * Connection: keep-alive
         * tabular-txid: 0
         * tabular-api-version-id: 1
         * tabular-max-keys: 1000
         * tabular-next-key: 0
         * Authorization: AWS4-HMAC-SHA256 Credential=pIX3SzyuQVmdrIVZnyy0/20220411/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0cd36e09c5e96e5485eee9cae7ba9c777515cbc365fc4f1d4403fb4b148c735a
         * x-amz-date: 20220411T071306Z
         * x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
         *
         * GET /bucket-for-tabular-api?schema HTTP/1.1
         * Host: localhost:9090
         * user-agent: VAST Trino client
         * x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
         * X-Amz-Date: 20220411T071306Z
         * tabular-max-keys: 1000
         * tabular-txid: 0
         * Authorization: AWS4-HMAC-SHA256 Credential=pIX3SzyuQVmdrIVZnyy0/20220411/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=eb453bab6c7ef20568c2bab64b22b3d526c4a1098943be9c13f542291d7abc98
         * tabular-api-version-id: 1
         * tabular-next-key: 0
         */
        VastConfig config = new VastConfig()
                .setEndpoint(URI.create("http://localhost:9090"))
                .setRegion("us-east-1")
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps");

        Request request = new VastRequestBuilder(config, GET, "/bucket-for-tabular-api", "schema")
                .setDate(new Date(1649661186000L)) // (aka 20220411T071306Z)
                .build();
        Multimap<String, String> headers = request.getHeaders();
        assertThat(headers.get("x-amz-content-sha256")).isEqualTo(ImmutableList.of("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
        assertThat(headers.get("X-Amz-Date")).isEqualTo(ImmutableList.of("20220411T071306Z"));
        assertThat(headers.get("Host")).isEqualTo(ImmutableList.of("localhost:9090"));
        assertThat(headers.get("Authorization")).isEqualTo(ImmutableList.of(
                "AWS4-HMAC-SHA256 Credential=pIX3SzyuQVmdrIVZnyy0/20220411/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
                "Signature=0cd36e09c5e96e5485eee9cae7ba9c777515cbc365fc4f1d4403fb4b148c735a"));
    }

    @Test
    public void testListBuckets()
            throws VastIOException
    {
        VastClient vastClient = getVastClient();
        int numberOfRetries = 3;
        String bucketNamePrefix = "testbucket";
        Map<String, Set<MockMapSchema>> testMockServerSchema = new HashMap<>(numberOfRetries);
        for (int i = 0; i < numberOfRetries; i++) {
            if (i > 0) {
                testMockServerSchema.put(bucketNamePrefix + i, ImmutableSet.of(MockMapSchema.empty("schema" + i)));
                handler.setSchema(testMockServerSchema);
            }
            List<String> replyBuckets = vastClient.listBuckets(true);
            assertEquals(replyBuckets.size(), i + 1);
            for (int j = 1; j < i; j++) {
                String bucketName = bucketNamePrefix + j;
                assertTrue(replyBuckets.contains(bucketName), format("Failed on iteration no. %d: %s not in list", i, bucketName));
            }
            assertTrue(replyBuckets.contains(BIG_CATALOG_BUCKET));
        }
    }

    private VastClient getVastClient()
    {
        return getVastClient(new JettyHttpClient());
    }

    private VastClient getVastClient(HttpClient httpClient)
    {
        VastConfig vastConfig = new VastConfig();
        vastConfig.setEngineVersion("1.2.3");
        return new VastClient(httpClient, getMockServerReadyVastConfig(), new DummyDependenciesFactory(vastConfig));
    }

    private VastConfig getMockServerReadyVastConfig()
    {
        return new VastConfig()
                .setEndpoint(URI.create(format("http://localhost:%d", testPort)))
                .setRegion("us-east-1")
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps")
                .setRetryMaxCount(RETRY_MAX_COUNT)
                .setRetrySleepDuration(1);
    }

    @Test(expectedExceptions = VastUserException.class)
    public void testCreateSchemaInvalidNameFails()
            throws VastException
    {
        VastClient unit = getVastClient();
        unit.createSchema(mockTransactionHandle, "nobucket", "{}");
    }

    @Test
    public void testGracefulCreateSchema()
            throws VastException, IOException
    {
        VastClient unit = getVastClient();
        MockUtils mockUtils = new MockUtils();
        String testBucket = "testCreateSchemaBucket";
        String testSchema = "testCreateSchemaSchema";
        mockUtils.createBucket(this.testPort, testBucket);
        unit.createSchema(mockTransactionHandle, format("%s/%s", testBucket, testSchema), "{}");
    }

    @Test(expectedExceptions = VastUserException.class, expectedExceptionsMessageRegExp = ".*HTTP Error: 404\\. Code: NoSuchKey\\. Message: The specified key does not exist\\. Resource: aresource\\. RequestId: a00100000025\\..*")
    public void testCreateSchemaUserError()
            throws VastException, IOException
    {
        testCreateSchemaError("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Resource>aresource</Resource><RequestId>a00100000025</RequestId></Error>", 404);
    }

    @Test(expectedExceptions = VastConflictException.class, expectedExceptionsMessageRegExp = ".*HTTP Error: 409\\. Code: InvalidBucketState\\. Message: The request is not valid with the current state of the bucket\\. Resource: api-create-schema-without-db\\. RequestId: a001000003ae\\..*")
    public void testCreateSchemaConflictError()
            throws VastException, IOException
    {
        testCreateSchemaError("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidBucketState</Code><Message>The request is not valid with the current state of the bucket.</Message><Resource>api-create-schema-without-db</Resource><RequestId>a001000003ae</RequestId></Error>", 409);
    }

    @Test(expectedExceptions = VastServerException.class, expectedExceptionsMessageRegExp = ".*HTTP Error: 503\\. Code: SlowDown\\. Message: Slow Down\\. RequestId: a00100000025\\..*")
    public void testCreateSchemaServerError()
            throws VastException, IOException
    {
        testCreateSchemaError("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code><Message>Slow Down</Message><Resource/><RequestId>a00100000025</RequestId></Error>", 503);
    }

    private void testCreateSchemaError(String message, int rCode)
            throws IOException, VastException
    {
        VastClient unit = getVastClient();
        MockUtils mockUtils = new MockUtils();
        String testBucket = format("bucket%s", rCode);
        String testSchema = format("schema%s", rCode);
        mockUtils.createBucket(this.testPort, testBucket);
        String path = format("%s/%s", testBucket, testSchema);
        HttpMethodName method = HttpMethodName.POST;
        Consumer<HttpExchange> action = httpExchange -> {
            try {
                httpExchange.sendResponseHeaders(rCode, message.length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        handler.setHook(format("/%s", path), method, action);
        unit.createSchema(mockTransactionHandle, path, "{}");
    }

    @DataProvider
    public Object[][] errors()
    {
        String positiveTestExceptionMessage = "java.io.EOFException: HttpConnectionOverHTTP@b5346ca::SocketChannelEndPoint@2114fb9e{l=/172.19.223.199:57762,r=/172.19.199.3:80,ISHUT,fill=-,flush=-,to=0/50000}{io=0/0,kio=0,kro=1}->HttpConnectionOverHTTP@b5346ca(l:/172.19.223.199:57762 <-> r:/172.19.199.3:80,closed=false)=>HttpChannelOverHTTP@3e096caf(exchange=HttpExchange@5c9b040d{req=HttpRequest[PUT /agoda-wa-table/wa-schema/webtraffic HTTP/1.1]@4def7c42[TERMINATED/null] res=HttpResponse[null 0 null]@4ba1b66a[PENDING/null]})[send=HttpSenderOverHTTP@3e38d7a5(req=QUEUED,snd=COMPLETED,failure=null)[HttpGenerator@3a070703{s=START}],recv=HttpReceiverOverHTTP@c3c716d(rsp=IDLE,failure=null)[HttpParser{s=CLOSED,0 of -1}]]";
        String negativeTestExceptionMessage = "java.io.EOFException: HttpConnectionOverHTTP@b5346ca::SocketChannelEndPoint@2114fb9e{l=/172.19.223.199:57762,r=/172.19.199.3:80,ISHUT,fill=-,flush=-,to=12345/50000}{io=0/0,kio=0,kro=1}->HttpConnectionOverHTTP@b5346ca(l:/172.19.223.199:57762 <-> r:/172.19.199.3:80,closed=false)=>HttpChannelOverHTTP@3e096caf(exchange=HttpExchange@5c9b040d{req=HttpRequest[PUT /agoda-wa-table/wa-schema/webtraffic HTTP/1.1]@4def7c42[TERMINATED/null] res=HttpResponse[null 0 null]@4ba1b66a[PENDING/null]})[send=HttpSenderOverHTTP@3e38d7a5(req=QUEUED,snd=COMPLETED,failure=null)[HttpGenerator@3a070703{s=START}],recv=HttpReceiverOverHTTP@c3c716d(rsp=IDLE,failure=null)[HttpParser{s=CLOSED,0 of -1}]]";

        return new Object[][] {{positiveTestExceptionMessage, RETRY_MAX_COUNT + 1},
                {negativeTestExceptionMessage, 1}};
    }

    @Test(dataProvider = "errors")
    public void testEOFUnsentClientSideFailure_ORION_106029(String errorMessage, int expectedNumOfRetries)
    {
        VastClient unit = getVastClient(mockHttpClient);
        when(mockHttpClient.execute(any(Request.class), any(VastResponseHandler.class))).thenThrow(new RuntimeException(new EOFException(errorMessage)));
        try {
            unit.listBuckets(true);
            fail("Expected an exception");
        }
        catch (Throwable any) {
            any.printStackTrace();
            // that's fine
        }
        verify(mockHttpClient, times(expectedNumOfRetries)).execute(any(Request.class), any(VastResponseHandler.class));
    }
}
