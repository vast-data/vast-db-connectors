package com.vastdata.vdb.sdk;

import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.ArrowSchemaUtils;

public class NoExternalRowIdColumnException
        extends VastUserException
{
    public NoExternalRowIdColumnException()
    {
        super("No external rowid (\"" + ArrowSchemaUtils.VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME + "\") column found in the table.");
    }
}
