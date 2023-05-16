/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

interface IterationResult
{
    boolean isTruncated();

    Long getNextKey();
}
