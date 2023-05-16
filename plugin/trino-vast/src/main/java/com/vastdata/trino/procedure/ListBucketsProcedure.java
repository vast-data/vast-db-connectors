/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.procedure;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastIOException;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;

public class ListBucketsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle LIST_BUCKETS = methodHandle(
            ListBucketsProcedure.class,
            "listBuckets",
            ConnectorSession.class,
            ConnectorAccessControl.class);

    private static final Logger LOG = Logger.get(ListBucketsProcedure.class);

    private final VastClient client;

    @Inject
    public ListBucketsProcedure(VastClient client)
    {
        this.client = client;
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "rpc",
                "list_buckets",
                ImmutableList.of(),
                LIST_BUCKETS.bindTo(this));
    }

    // called via reflection
    public void listBuckets(ConnectorSession session, ConnectorAccessControl accessControl)
    {
        try {
            LOG.debug("listing buckets: %s", client.listBuckets(true));
        }
        catch (VastIOException e) {
            throw toRuntime(e);
        }
    }
}
