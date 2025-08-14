/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vastdata.client.tx.SimpleVastTransaction;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class VastTransactionHandle extends SimpleVastTransaction implements ConnectorTransactionHandle
{

    @JsonCreator
    public VastTransactionHandle(@JsonProperty("id") long id)
    {
        super(id);
    }

    @Override
    @JsonProperty("id")
    public long getId()
    {
        return super.getId();
    }



    @Override
    public String toString()
    {
        return "VastTransactionHandle{" +
                "id='" + getId() + '\'' +
                '}';
    }
}
