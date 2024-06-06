/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb.alter;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.tx.VastTransaction;

import java.util.stream.Stream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public abstract class AbstractColumnsChange implements VastTableChange
{
    protected final Stream<TableColumnLifecycleContext> columns;

    public AbstractColumnsChange(Stream<TableColumnLifecycleContext> columns) {
        this.columns = columns;
    }

    @Override
    public void accept(VastClient vastClient, VastTransaction vastTransaction)
    {
        columns.forEach(columnCtx -> {
            try {
                executeAction(vastClient, vastTransaction, columnCtx);
            }
            catch (VastException e) {
                throw toRuntime(e);
            }
        });
    }

    protected abstract void executeAction(VastClient vastClient, VastTransaction vastTransaction, TableColumnLifecycleContext columnCtx) throws VastException;
}
