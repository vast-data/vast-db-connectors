/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.metrics;

import com.vastdata.client.metrics.VastMetrics;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class PageSinkMetrics
        implements VastMetrics<PageSinkMetrics>
{
    protected final LongAdder appendPageRegularIdleTimeNanos = new LongAdder();
    protected final LongAdder appendPageRegularIdleTimeCount = new LongAdder();

    protected final LongAdder appendPageRegularExecTimeNanos = new LongAdder();
    protected final LongAdder appendPageRegularExecTimeCount = new LongAdder();

    protected final LongAdder finishIdleTimeNanos = new LongAdder();
    protected final LongAdder finishIdleTimeCount = new LongAdder();

    protected final LongAdder finishExecTimeNanos = new LongAdder();
    protected final LongAdder finishExecTimeCount = new LongAdder();

    protected final LongAdder incomingPageSize = new LongAdder();
    protected final LongAdder incomingPageCount = new LongAdder();

    protected final LongAdder pageSinkCreated = new LongAdder();
    protected final LongAdder pageSinkClosed = new LongAdder();

    protected final LongAdder vsrBuildTimeNanos = new LongAdder();
    protected final LongAdder vsrBuildTimeCount = new LongAdder();

    protected final LongAdder pageCopyPositionTimeNanos = new LongAdder();
    protected final LongAdder pageCopyPositionTimeCount = new LongAdder();

    public void recordAppendPageIdleTime(long elapsedNanos)
    {
        appendPageRegularIdleTimeNanos.add(elapsedNanos);
        appendPageRegularIdleTimeCount.increment();
    }

    public void recordAppendPageExecTime(long elapsedNanos)
    {
        appendPageRegularExecTimeNanos.add(elapsedNanos);
        appendPageRegularExecTimeCount.increment();
    }

    public void recordFinishIdleTime(long elapsedNanos)
    {
        finishIdleTimeNanos.add(elapsedNanos);
        finishIdleTimeCount.increment();
    }

    public void recordFinishExecTime(long elapsedNanos)
    {
        finishExecTimeNanos.add(elapsedNanos);
        finishExecTimeCount.increment();
    }

    public void recordIncomingPage(long size)
    {
        incomingPageSize.add(size);
        incomingPageCount.increment();
    }

    public void incPageSinkCreated()
    {
        pageSinkCreated.increment();
    }

    public void incPageSinkClosed()
    {
        pageSinkClosed.increment();
    }

    public void recordVsrBuildTime(long elapsedNanos)
    {
        vsrBuildTimeNanos.add(elapsedNanos);
        vsrBuildTimeCount.increment();
    }

    public void recordPageCopyPositionTime(long elapsedNanos)
    {
        pageCopyPositionTimeNanos.add(elapsedNanos);
        pageCopyPositionTimeCount.increment();
    }

    @Managed
    public long getAppendPageRegularIdleTimeNanos()
    {
        return appendPageRegularIdleTimeNanos.sum();
    }

    @Managed
    public long getAppendPageRegularIdleTimeCount()
    {
        return appendPageRegularIdleTimeCount.sum();
    }

    @Managed
    public long getAppendPageRegularExecTimeNanos()
    {
        return appendPageRegularExecTimeNanos.sum();
    }

    @Managed
    public long getAppendPageRegularExecTimeCount()
    {
        return appendPageRegularExecTimeCount.sum();
    }

    @Managed
    public long getFinishIdleTimeNanos()
    {
        return finishIdleTimeNanos.sum();
    }

    @Managed
    public long getFinishIdleTimeCount()
    {
        return finishIdleTimeCount.sum();
    }

    @Managed
    public long getFinishExecTimeNanos()
    {
        return finishExecTimeNanos.sum();
    }

    @Managed
    public long getFinishExecTimeCount()
    {
        return finishExecTimeCount.sum();
    }

    @Managed
    public long getIncomingPageSize()
    {
        return incomingPageSize.sum();
    }

    @Managed
    public long getIncomingPageCount()
    {
        return incomingPageCount.sum();
    }

    @Managed
    public long getPageSinkCreated()
    {
        return pageSinkCreated.sum();
    }

    @Managed
    public long getPageSinkClosed()
    {
        return pageSinkClosed.sum();
    }

    @Managed
    public long getVsrBuildTimeNanos()
    {
        return vsrBuildTimeNanos.sum();
    }

    @Managed
    public long getVsrBuildTimeCount()
    {
        return vsrBuildTimeCount.sum();
    }

    @Managed
    public long getPageCopyPositionTimeNanos()
    {
        return pageCopyPositionTimeNanos.sum();
    }

    @Managed
    public long getPageCopyPositionTimeCount()
    {
        return pageCopyPositionTimeCount.sum();
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("appendPageRegularIdleTimeNanos",
                getAppendPageRegularIdleTimeNanos());
        map.put("appendPageRegularIdleTimeCount",
                getAppendPageRegularIdleTimeCount());
        map.put("appendPageRegularExecTimeNanos",
                getAppendPageRegularExecTimeNanos());
        map.put("appendPageRegularExecTimeCount",
                getAppendPageRegularExecTimeCount());
        map.put("finishIdleTimeNanos", getFinishIdleTimeNanos());
        map.put("finishIdleTimeCount", getFinishIdleTimeCount());
        map.put("finishExecTimeNanos", getFinishExecTimeNanos());
        map.put("finishExecTimeCount", getFinishExecTimeCount());
        map.put("incomingPageSize", getIncomingPageSize());
        map.put("incomingPageCount", getIncomingPageCount());
        map.put("pageSinkCreated", getPageSinkCreated());
        map.put("pageSinkClosed", getPageSinkClosed());
        map.put("vsrBuildTimeNanos", getVsrBuildTimeNanos());
        map.put("vsrBuildTimeCount", getVsrBuildTimeCount());
        map.put("pageCopyPositionTimeNanos", getPageCopyPositionTimeNanos());
        map.put("pageCopyPositionTimeCount", getPageCopyPositionTimeCount());
        return map;
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("appendPageRegularIdleTimeNanos",
                getAppendPageRegularIdleTimeNanos());
        map.put("appendPageRegularIdleTimeCount",
                getAppendPageRegularIdleTimeCount());
        map.put("appendPageRegularExecTimeNanos",
                getAppendPageRegularExecTimeNanos());
        map.put("appendPageRegularExecTimeCount",
                getAppendPageRegularExecTimeCount());
        map.put("finishIdleTimeNanos", getFinishIdleTimeNanos());
        map.put("finishIdleTimeCount", getFinishIdleTimeCount());
        map.put("finishExecTimeNanos", getFinishExecTimeNanos());
        map.put("finishExecTimeCount", getFinishExecTimeCount());
        map.put("incomingPageSize", getIncomingPageSize());
        map.put("incomingPageCount", getIncomingPageCount());
        map.put("pageSinkCreated", getPageSinkCreated());
        map.put("pageSinkClosed", getPageSinkClosed());
        map.put("vsrBuildTimeNanos", getVsrBuildTimeNanos());
        map.put("vsrBuildTimeCount", getVsrBuildTimeCount());
        map.put("pageCopyPositionTimeNanos", getPageCopyPositionTimeNanos());
        map.put("pageCopyPositionTimeCount", getPageCopyPositionTimeCount());
        return map;
    }

    @Override
    public Map<String, Long> stateMetrics()
    {
        return new HashMap<>();
    }

    @Override
    public Map<String, Long> durationMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("appendPageRegularIdleTimeNanos",
                getAppendPageRegularIdleTimeNanos());
        map.put("appendPageRegularExecTimeNanos",
                getAppendPageRegularExecTimeNanos());
        map.put("finishIdleTimeNanos", getFinishIdleTimeNanos());
        map.put("finishExecTimeNanos", getFinishExecTimeNanos());
        map.put("vsrBuildTimeNanos", getVsrBuildTimeNanos());
        map.put("pageCopyPositionTimeNanos", getPageCopyPositionTimeNanos());
        return map;
    }

    @Override
    public void merge(PageSinkMetrics other)
    {
        if (other == null || other == this) {
            return;
        }
        this.appendPageRegularIdleTimeNanos.add(
                other.appendPageRegularIdleTimeNanos.sum());
        this.appendPageRegularIdleTimeCount.add(
                other.appendPageRegularIdleTimeCount.sum());
        this.appendPageRegularExecTimeNanos.add(
                other.appendPageRegularExecTimeNanos.sum());
        this.appendPageRegularExecTimeCount.add(
                other.appendPageRegularExecTimeCount.sum());
        this.finishIdleTimeNanos.add(other.finishIdleTimeNanos.sum());
        this.finishIdleTimeCount.add(other.finishIdleTimeCount.sum());
        this.finishExecTimeNanos.add(other.finishExecTimeNanos.sum());
        this.finishExecTimeCount.add(other.finishExecTimeCount.sum());
        this.incomingPageSize.add(other.incomingPageSize.sum());
        this.incomingPageCount.add(other.incomingPageCount.sum());
        this.pageSinkCreated.add(other.pageSinkCreated.sum());
        this.pageSinkClosed.add(other.pageSinkClosed.sum());
        this.vsrBuildTimeNanos.add(other.vsrBuildTimeNanos.sum());
        this.vsrBuildTimeCount.add(other.vsrBuildTimeCount.sum());
        this.pageCopyPositionTimeNanos.add(
                other.pageCopyPositionTimeNanos.sum());
        this.pageCopyPositionTimeCount.add(
                other.pageCopyPositionTimeCount.sum());
    }

    @Override
    public PageSinkMetrics copy()
    {
        PageSinkMetrics clone = new PageSinkMetrics();
        clone.appendPageRegularIdleTimeNanos.add(
                this.appendPageRegularIdleTimeNanos.sum());
        clone.appendPageRegularIdleTimeCount.add(
                this.appendPageRegularIdleTimeCount.sum());
        clone.appendPageRegularExecTimeNanos.add(
                this.appendPageRegularExecTimeNanos.sum());
        clone.appendPageRegularExecTimeCount.add(
                this.appendPageRegularExecTimeCount.sum());
        clone.finishIdleTimeNanos.add(this.finishIdleTimeNanos.sum());
        clone.finishIdleTimeCount.add(this.finishIdleTimeCount.sum());
        clone.finishExecTimeNanos.add(this.finishExecTimeNanos.sum());
        clone.finishExecTimeCount.add(this.finishExecTimeCount.sum());
        clone.incomingPageSize.add(this.incomingPageSize.sum());
        clone.incomingPageCount.add(this.incomingPageCount.sum());
        clone.pageSinkCreated.add(this.pageSinkCreated.sum());
        clone.pageSinkClosed.add(this.pageSinkClosed.sum());
        clone.vsrBuildTimeNanos.add(this.vsrBuildTimeNanos.sum());
        clone.vsrBuildTimeCount.add(this.vsrBuildTimeCount.sum());
        clone.pageCopyPositionTimeNanos.add(
                this.pageCopyPositionTimeNanos.sum());
        clone.pageCopyPositionTimeCount.add(
                this.pageCopyPositionTimeCount.sum());
        return clone;
    }
}
