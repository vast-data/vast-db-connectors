/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.airlift.log.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VastMockS3Server
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(VastMockS3Server.class);
    private final int port;
    private final HttpHandler handler;
    private HttpServer server;
    private ExecutorService executor;

    public VastMockS3Server()
    {
        this(0);
    }

    public VastMockS3Server(int port)
    {
        this.port = port;
        this.handler = new VastRootHandler();
    }

    public VastMockS3Server(int port, HttpHandler handler)
    {
        this.port = port;
        this.handler = handler;
    }

    public int start()
            throws IOException
    {
        LOG.info(String.format("STARTING VastMockS3Server on port %s", port));
        server = HttpServer.create(new InetSocketAddress(port), 10);
        server.createContext("/", handler);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
        return server.getAddress().getPort();
    }

    @Override
    public void close()
    {
        LOG.info("Stopping server");
        if (Objects.nonNull(server)) {
            server.stop(2);
        }
        executor.shutdownNow();
        LOG.info("Stopped server");
    }
}
