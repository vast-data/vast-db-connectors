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
    public VastTransactionHandle(@JsonProperty("id") long id,
            @JsonProperty("readOnly") boolean readOnly,
            @JsonProperty("autoCommit") boolean autocommit)
    {
        super(id, readOnly, autocommit);
    }

    @Override
    @JsonProperty("id")
    public long getId()
    {
        return super.getId();
    }

    @Override
    @JsonProperty("readOnly")
    public boolean isReadOnly()
    {
        return super.isReadOnly();
    }

    @JsonProperty("autoCommit")
    public boolean isAutocommit()
    {
        return super.isAutocommit();
    }

    @Override
    public String toString()
    {
        return "VastTransactionHandle{" +
                "id='" + getId() + '\'' +
                ", readOnly=" + isReadOnly() +
                ", autocommit=" + isAutocommit() +
                '}';
    }
}
