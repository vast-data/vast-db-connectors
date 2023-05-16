/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.mockserver.MockMapSchema;
import io.airlift.log.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public abstract class AbstractRequestConsumer
        implements Consumer<HttpExchange>
{
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    private static final Logger LOG = Logger.get(AbstractRequestConsumer.class);

    protected final Map<String, Set<MockMapSchema>> schemaMap;

    protected final Set<String> openTransactions;

    protected AbstractRequestConsumer(Map<String, Set<MockMapSchema>> schema, Set<String> openTransactions)
    {
        this.schemaMap = schema;
        this.openTransactions = openTransactions;
    }

    @Override
    public void accept(HttpExchange httpExchange)
    {
        try {
            handle(httpExchange);
        }
        catch (Throwable e) {
            String format = String.format("Unexpected error: %s", e);
            try {
                LOG.error(e, "Unexpected Error");
                respondError(format, httpExchange);
            }
            catch (IOException ex) {
                LOG.error(ex, String.format("Failed replying with error: %s", format));
            }
        }
    }

    protected abstract void handle(HttpExchange httpExchange)
            throws Exception;

    protected void respondError(String message, HttpExchange he)
            throws IOException
    {
        respond(message, he, 500);
    }

    protected void respondOK(HttpExchange he)
            throws IOException
    {
        respond("", he, 200);
    }

    protected void respondError(HttpExchange he)
            throws IOException
    {
        respond("<h1>Bad URL</h1>", he, 404);
    }

    protected void respond(Consumer<OutputStream> writer, HttpExchange he)
            throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writer.accept(bos);
        byte[] b = bos.toByteArray();
        respond(b, he, 200);
    }

    protected void respond(String response, HttpExchange he, int rCode)
            throws IOException
    {
        respond(response.getBytes(Charset.defaultCharset()), he, rCode);
    }

    protected void respond(byte[] bytes, HttpExchange he, int rCode)
            throws IOException
    {
        he.getResponseHeaders().put("tabular-next-key", ImmutableList.of("0"));
        he.getResponseHeaders().put("tabular-is-truncated", ImmutableList.of("false"));
        he.sendResponseHeaders(rCode, bytes.length);
        try (OutputStream os = he.getResponseBody()) {
            os.write(bytes);
        }
    }

    protected static byte[] readAllBytes(InputStream is)
            throws IOException
    {
        return readBytesFromStream(is, Integer.MAX_VALUE);
    }
    
    private static byte[] readBytesFromStream(InputStream is, int remaining)
            throws IOException
    {
        List<byte[]> bufs = null;
        byte[] result = null;
        int total = 0;
        int n;
        do {
            byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
            int nread = 0;

            // read to EOF which may read more or less than buffer size
            while ((n = is.read(buf, nread,
                    Math.min(buf.length - nread, remaining))) > 0) {
                nread += n;
                remaining -= n;
            }

            if (nread > 0) {
                if (MAX_BUFFER_SIZE - total < nread) {
                    throw new OutOfMemoryError("Required array size too large");
                }
                total += nread;
                if (result == null) {
                    result = buf;
                } else {
                    if (bufs == null) {
                        bufs = new ArrayList<>();
                        bufs.add(result);
                    }
                    bufs.add(buf);
                }
            }
            // if the last call to read returned -1 or the number of bytes
            // requested have been read then break
        } while (n >= 0 && remaining > 0);

        if (bufs == null) {
            if (result == null) {
                return new byte[0];
            }
            return result.length == total ?
                    result : Arrays.copyOf(result, total);
        }

        result = new byte[total];
        int offset = 0;
        remaining = total;
        for (byte[] b : bufs) {
            int count = Math.min(b.length, remaining);
            System.arraycopy(b, 0, result, offset, count);
            offset += count;
            remaining -= count;
        }

        return result;
    }

    protected Optional<String> getTransactionHeader(HttpExchange he)
    {
        return he.getRequestHeaders().containsKey(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()) ?
                Optional.of(Iterables.getOnlyElement(he.getRequestHeaders().get(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()))) :
                Optional.empty();
    }

    protected void removeTransaction(HttpExchange he)
    {
        Optional<String> transactionHeader = getTransactionHeader(he);
        if (transactionHeader.isPresent()) {
            String txid = transactionHeader.get();
            try {
                if (this.openTransactions.remove(txid)) {
                    respondOK(he);
                }
                else {
                    respondError(String.format("%s is not open", txid), he);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            try {
                respondError("Transaction ID was not provided", he);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
//        transactionHeader.ifPresentOrElse(txid -> {
//            try {
//                if (this.openTransactions.remove(txid)) {
//                    respondOK(he);
//                }
//                else {
//                    respondError(String.format("%s is not open", txid), he);
//                }
//            }
//            catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }, () -> {
//            try {
//                respondError("Transaction ID was not provided", he);
//            }
//            catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
    }
}
