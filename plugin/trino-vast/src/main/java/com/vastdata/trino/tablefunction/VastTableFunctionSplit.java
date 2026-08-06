/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import io.trino.spi.connector.ConnectorSplit;

public record VastTableFunctionSplit(byte[] queryId,
        byte[] ticket,
        byte[] schema)
        implements ConnectorSplit
{}
