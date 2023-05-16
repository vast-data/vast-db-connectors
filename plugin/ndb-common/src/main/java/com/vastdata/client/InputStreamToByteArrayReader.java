/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InputStreamToByteArrayReader
{

    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;


    public byte[] readAllBytes(InputStream inputStream)
    {
        return readNBytes(inputStream, Integer.MAX_VALUE);
    }

    public byte[] readNBytes(InputStream inputStream, Integer length)
    {
        try {
            return readBytesFromStream(inputStream, length);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
