/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.vastdata.client.InputStreamToByteArrayReader;
import io.airlift.log.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import vast_flatbuf.tabular.ListSchemasResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVastMockS3Server
{
    private static final Logger LOG = Logger.get(TestVastMockS3Server.class);

    private static VastMockS3Server mockServer;
    private static final VastRootHandler handler = new VastRootHandler();
    private int testPort;
    private final InputStreamToByteArrayReader inputStreamToByteArrayReader = new InputStreamToByteArrayReader();

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

    @BeforeTest
    public void clearHandler()
    {
        handler.clearSchema();
    }

    @Test
    public void testStart()
            throws Exception
    {
        LOG.info("Starting server");
        URL url = new URL(String.format("http://localhost:%d/", testPort));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setConnectTimeout(2000);
        con.setReadTimeout(2000);
        Optional<String> response;
        try (InputStream inputStream = con.getInputStream()) {
            byte[] bytes = inputStreamToByteArrayReader.readAllBytes(inputStream);
            response = Optional.of(new String(bytes, Charset.defaultCharset()));
        }
        assertEquals(con.getResponseCode(), 200);
        assertTrue(response.get().startsWith(MockListBucketsReply.LIST_BUCKETS_REPLY_PREFIX), response.get());
    }

    @Test
    public void testListSchema()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String bucket1 = "bucket1";
        mockUtils.createBucket(this.testPort, bucket1);
        ListSchemasResponse bucketSchemas = mockUtils.getBucketSchemas(this.testPort, bucket1);
        assertEquals(bucketSchemas.schemasLength(), 0);
    }
}
