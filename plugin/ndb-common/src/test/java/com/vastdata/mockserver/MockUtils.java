/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.vastdata.client.InputStreamToByteArrayReader;
import vast_flatbuf.tabular.ListSchemasResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;

public class MockUtils
{
    private final InputStreamToByteArrayReader inputStreamToByteArrayReader = new InputStreamToByteArrayReader();

    public void createBucket(int port, String bucketName)
            throws IOException
    {
        URL url = new URL(String.format("http://localhost:%d/" + bucketName, port));
        HttpURLConnection con = null;
        try {
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setConnectTimeout(10000);
            con.setReadTimeout(10000);
            assertEquals(con.getResponseCode(), 200);
            con.disconnect();
        }
        finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }

    public ListSchemasResponse getBucketSchemas(int port, String bucketName)
            throws IOException
    {
        URL url = new URL(String.format("http://localhost:%d/" + bucketName, port));
        HttpURLConnection con = null;
        try {
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(10000);
            con.setReadTimeout(10000);
            ByteBuffer bb;
            try (InputStream inputStream = con.getInputStream()) {
                byte[] bytes = inputStreamToByteArrayReader.readAllBytes(inputStream);
                bb = ByteBuffer.wrap(bytes);
            }
            catch (Exception any) {
                throw new IOException(any);
            }
            assertEquals(con.getResponseCode(), 200);
            return ListSchemasResponse.getRootAsListSchemasResponse(bb);
        }
        finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }
}
