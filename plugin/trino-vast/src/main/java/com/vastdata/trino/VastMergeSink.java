/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastTooLargePageException;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.trino.VastMergePage.createVastUpdateDeleteInsertPages;
import static com.vastdata.trino.VastSessionProperties.*;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class VastMergeSink
        implements ConnectorMergeSink
{
    private static final Logger LOG = Logger.get(VastMergeSink.class);
    private final VastClient client;
    private final VastTransactionHandle transactionHandle;
    private final VastMergeTableHandle mergeHandle;
    private final ConnectorSession session;
    private final VastRecordBatchBuilder builder;

    private static VastRecordBatchBuilder recordBatchBuilder(VastTableHandle table, boolean forDelete)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.add(ROW_ID_FIELD); // row ID column is expected to be the first one
        if (! forDelete) {
            table.getMergedColumns().stream().map(VastColumnHandle::getField).forEach(fields::add);
        }
        return new VastRecordBatchBuilder(new Schema(fields.build()));
    }

    public VastMergeSink(VastClient client, ConnectorSession session, VastTransactionHandle transactionHandle, VastMergeTableHandle mergeHandle)
    {
        this.client = client;
        this.transactionHandle = transactionHandle;
        this.mergeHandle = mergeHandle;
        List<VastColumnHandle> columns = mergeHandle.getColumns();
        List<Field> fields = columns.stream().map(VastColumnHandle::getField).collect(Collectors.toList());
        this.session = session;
        this.builder = new VastRecordBatchBuilder(new Schema(fields));
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int columnCount = mergeHandle.getColumns().size();
        LOG.debug("storeMergeRows: mergableColumns: %s, page column count: %s", mergeHandle.getColumns(), page.getChannelCount());
        VastMergePage vastMergePage = createVastUpdateDeleteInsertPages(page, columnCount);

        // delete
        Optional<Page> optionalDeletePage = vastMergePage.getDeletePage();
        optionalDeletePage.ifPresent(deletePage -> {
            LOG.debug("deleteRows(%d IDs, %d bytes)", deletePage.getPositionCount(), deletePage.getSizeInBytes());
            try (VectorSchemaRoot root = recordBatchBuilder(mergeHandle.getTable(), true).build(deletePage)) {
                LOG.debug("Before executing delete rpc");
                client.deleteRows(
                        this.transactionHandle,
                        this.mergeHandle.getTable().getSchemaName(),
                        this.mergeHandle.getTable().getTableName(),
                        root,
                        getDataEndpoints(this.session).get(0), //TODO check
                        Optional.of(getMaxRowsPerDelete(this.session))
                );
            }
            catch (VastTooLargePageException e) {
                throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
            }
            catch (VastException e) {
                throw toRuntime(e);
            }
        });

        // insert
        Optional<Page> optionalInsertPage = vastMergePage.getInsertPage();
        optionalInsertPage.ifPresent(insertPage -> {
            try (VectorSchemaRoot root = builder.build(insertPage)) {
                client.insertRows(
                        this.transactionHandle,
                        this.mergeHandle.getTable().getSchemaName(),
                        this.mergeHandle.getTable().getTableName(),
                        root,
                        getDataEndpoints(this.session).get(0),
                        Optional.of(getMaxRowsPerInsert(this.session))
                );
            }
            catch (VastTooLargePageException e) {
                throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
            }
            catch (VastException e) {
                throw toRuntime(e);
            }
        });

        // update
        Optional<Page> optionalUpdatePage = vastMergePage.getUpdatePage();
        optionalUpdatePage.ifPresent(updatePage -> {
            LOG.debug("updateRows(%d IDs, %d bytes)", updatePage.getPositionCount(), updatePage.getSizeInBytes());
            try (VectorSchemaRoot root = recordBatchBuilder(mergeHandle.getTable(), false).build(updatePage)) {
                client.updateRows(
                        this.transactionHandle,
                        this.mergeHandle.getTable().getSchemaName(),
                        this.mergeHandle.getTable().getTableName(),
                        root,
                        getDataEndpoints(this.session).get(0),
                        Optional.of(getMaxRowsPerUpdate(this.session))
                );
            }
            catch (VastTooLargePageException e) {
                throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
            }
            catch (VastException e) {
                throw toRuntime(e);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        LOG.warn("aborting merge sink");

    }
}
