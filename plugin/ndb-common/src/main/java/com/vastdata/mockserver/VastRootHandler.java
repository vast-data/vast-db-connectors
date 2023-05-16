/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.vastdata.mockserver.handle.PostHandler;
import com.vastdata.mockserver.handle.DeleteHandler;
import com.vastdata.mockserver.handle.GetHandler;
import com.vastdata.mockserver.handle.PutHandler;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.amazonaws.http.HttpMethodName.DELETE;
import static com.amazonaws.http.HttpMethodName.GET;
import static com.amazonaws.http.HttpMethodName.POST;
import static com.amazonaws.http.HttpMethodName.PUT;

public class VastRootHandler
        implements HttpHandler
{
    public static final Consumer<HttpExchange> THROWER = h -> {
        throw new RuntimeException("Request handler not implemented");
    };
    private static final Logger LOG = Logger.get(VastRootHandler.class);
    public static final String HANDLING = "Handling ";
    private final Map<String, Set<MockMapSchema>> schema = new HashMap<>();

    private final EnumMap<HttpMethodName, Consumer<HttpExchange>> handlersMap = new EnumMap<>(HttpMethodName.class);

    private final Map<HookKey, Optional<Consumer<HttpExchange>>> hooks = new HashMap<>();

    public VastRootHandler()
    {
        Set<String> openTransactions = Sets.newConcurrentHashSet();
        schema.put("vast-big-catalog-bucket", new HashSet<>());
        handlersMap.put(GET, new GetHandler(schema, openTransactions));
        handlersMap.put(POST, new PostHandler(schema, openTransactions));
        handlersMap.put(PUT, new PutHandler(schema, openTransactions));
        handlersMap.put(DELETE, new DeleteHandler(schema, openTransactions));
    }

    @Override
    public void handle(HttpExchange he)
    {
        URI requestURI = he.getRequestURI();
        LOG.info(HANDLING + requestURI);
        HttpMethodName method = HttpMethodName.fromValue(he.getRequestMethod());
        HookKey requestHookKey = new HookKey(requestURI.getPath(), method);
        Optional<Consumer<HttpExchange>> optionalHook = hooks.getOrDefault(requestHookKey, Optional.empty());
        if (optionalHook.isPresent()) {
            Consumer<HttpExchange> hook = optionalHook.get();
            LOG.info("Found hook for request: %s", requestHookKey);
            hook.accept(he);
        }
        else {
            handlersMap.getOrDefault(method, THROWER).accept(he);
        }
//        optionalHook.ifPresentOrElse(hook -> {
//                    LOG.info("Found hook for request: %s", requestHookKey);
//                    hook.accept(he);
//                }, () -> handlersMap.getOrDefault(method, THROWER).accept(he));
    }

    public void setSchema(Map<String, Set<MockMapSchema>> schema)
    {
        clearSchema();
        this.schema.putAll(schema);
    }

    public void clearSchema()
    {
        this.schema.clear();
        this.hooks.clear();
    }

    public void setHook(String path, HttpMethodName method, Consumer<HttpExchange> action)
    {
        this.hooks.put(new HookKey(path, method), Optional.of(action));
    }
}
