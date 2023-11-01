/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb.alter;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.tx.VastTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class AddColumn extends AbstractColumnsChange
{
    private static final Logger LOG = LoggerFactory.getLogger(AddColumn.class);

    AddColumn(Stream<TableColumnLifecycleContext> columns) {
        super(columns);
    }

    @Override
    protected void executeAction(VastClient vastClient, VastTransaction vastTransaction, TableColumnLifecycleContext columnCtx)
        throws VastException
    {
        LOG.debug("AddColumn.executeAction() {}", columnCtx);
        vastClient.addColumn(vastTransaction, columnCtx);
    }
}
