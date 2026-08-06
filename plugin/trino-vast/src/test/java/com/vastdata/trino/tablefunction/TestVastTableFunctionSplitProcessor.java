/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import org.junit.jupiter.api.Test;

import static com.vastdata.trino.tablefunction.VastTableFunctionSplitProcessor.timeByMicroSeconds;
import static com.vastdata.trino.tablefunction.VastTableFunctionSplitProcessor.timeByMilliSeconds;
import static com.vastdata.trino.tablefunction.VastTableFunctionSplitProcessor.timeByNanoSeconds;
import static com.vastdata.trino.tablefunction.VastTableFunctionSplitProcessor.timeBySeconds;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVastTableFunctionSplitProcessor
{
    @Test
    public void testTimeBySecond()
    {
        long timeValue = 43201000000000000L;
        assertThat(timeBySeconds(timeValue)).isEqualTo("\"12:00:01\"");
    }

    @Test
    public void testTimeByMilliSecond()
    {
        long timeValue = 43201000000000000L;
        assertThat(timeByMilliSeconds(timeValue)).isEqualTo("\"12:00:01.000\"");
    }

    @Test
    public void testTimeByMicroSecond()
    {
        long timeValue = 43201000000000000L;
        assertThat(timeByMicroSeconds(timeValue)).isEqualTo(
                "\"12:00:01.000000\"");
    }

    @Test
    public void testTimeByNanoSecond()
    {
        long timeValue = 43201000000000000L;
        assertThat(timeByNanoSeconds(timeValue)).isEqualTo(
                "\"12:00:01.000000000\"");
    }
}
