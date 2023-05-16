/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import com.google.common.collect.ImmutableList;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BooleanSupplier;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestSubmittedWorkProcessor
{
    @Mock WorkExecutor<Object> mockWorkExecutor;

    @Test
    public void testCircuitBreaker()
    {
        initMocks(this);
        URI endpoint = URI.create("http://localhost:8080");
        LinkedBlockingDeque<WorkExecutor<Object>> queue = new LinkedBlockingDeque<>(ImmutableList.of(mockWorkExecutor, mockWorkExecutor));
        BooleanSupplier breaker = () -> queue.size() > 0;
        new SubmittedWorkProcessor<>(endpoint, queue, breaker).run();
        verify(mockWorkExecutor, times(1)).accept(endpoint);
    }

    @Test
    public void testGracefulRun()
    {
        initMocks(this);
        URI endpoint = URI.create("http://localhost:8080");
        LinkedBlockingDeque<WorkExecutor<Object>> queue = new LinkedBlockingDeque<>(ImmutableList.of(mockWorkExecutor, mockWorkExecutor));
        BooleanSupplier breaker = () -> true;
        new SubmittedWorkProcessor<>(endpoint, queue, breaker).run();
        verify(mockWorkExecutor, times(2)).accept(endpoint);
    }
}
