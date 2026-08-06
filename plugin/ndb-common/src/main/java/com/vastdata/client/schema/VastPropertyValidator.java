/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;

interface VastPropertyValidator
{
    void validate()
            throws VastUserException;
}
