/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb.alter;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.tx.VastTransaction;

import java.util.stream.Stream;

public class DropColumn extends AbstractColumnsChange
{
    DropColumn(Stream<TableColumnLifecycleContext> columns) {
        super(columns);
    }

    @Override
    protected void executeAction(VastClient vastClient, VastTransaction vastTransaction, TableColumnLifecycleContext columnCtx)
            throws VastException
    {
        vastClient.dropColumn(vastTransaction, columnCtx);
    }
}
