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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD;
import static com.vastdata.trino.VastMergePage.createVastUpdateDeleteInsertPages;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerDelete;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerInsert;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerUpdate;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static java.lang.String.format;
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
    private final List<URI> dataEndpoints;
    private long pageCount;

    private static VastRecordBatchBuilder recordBatchBuilder(VastTableHandle table, boolean forDelete)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.add(ROW_ID_FIELD); // row ID column is expected to be the first one
        if (! forDelete) {
            table.getMergedColumns().stream().map(VastColumnHandle::getField).forEach(fields::add);
        }
        return new VastRecordBatchBuilder(new Schema(fields.build()));
    }

    public VastMergeSink(VastClient client, ConnectorSession session, VastTransactionHandle transactionHandle, VastMergeTableHandle mergeHandle, List<URI> dataEndpoints)
    {
        this.client = client;
        this.transactionHandle = transactionHandle;
        this.mergeHandle = mergeHandle;
        List<VastColumnHandle> columns = mergeHandle.getColumns();
        List<Field> fields = columns.stream().map(VastColumnHandle::getField).collect(Collectors.toList());
        this.session = session;
        this.builder = new VastRecordBatchBuilder(new Schema(fields));
        this.dataEndpoints = dataEndpoints;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int columnCount = mergeHandle.getColumns().size();
        int dataEndPointsCount = this.dataEndpoints.size();
        LOG.debug("storeMergeRows: mergableColumns: %s, page column count: %s", mergeHandle.getColumns(), page.getChannelCount());
        VastMergePage vastMergePage = createVastUpdateDeleteInsertPages(page, columnCount);

        // delete
        Optional<Page> optionalDeletePage = vastMergePage.getDeletePage();
        optionalDeletePage.ifPresent(deletePage -> {
            LOG.debug("deleteRows(%d IDs, %d bytes)", deletePage.getPositionCount(), deletePage.getSizeInBytes());
            try (VectorSchemaRoot root = recordBatchBuilder(mergeHandle.getTable(), true).build(deletePage)) {
                client.deleteRows(
                        this.transactionHandle,
                        this.mergeHandle.getTable().getSchemaName(),
                        this.mergeHandle.getTable().getTableName(),
                        root,
                        dataEndpoints.get((int) pageCount % dataEndPointsCount),
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
                LOG.debug("insertRows(%d IDs, %d bytes)", insertPage.getPositionCount(), insertPage.getSizeInBytes());
                client.insertRows(
                        this.transactionHandle,
                        this.mergeHandle.getTable().getSchemaName(),
                        this.mergeHandle.getTable().getTableName(),
                        root,
                        dataEndpoints.get((int) pageCount % dataEndPointsCount),
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
                        dropExtraRowIdColumnFromUpdate(root),
                        dataEndpoints.get((int) pageCount % dataEndPointsCount),
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
        pageCount += 1;
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

    public List<URI> getShuffledDataEndpoints()
    {
        return this.dataEndpoints;
    }

    private VectorSchemaRoot dropExtraRowIdColumnFromUpdate(VectorSchemaRoot root)
    {
        List<FieldVector> vectors = root.getFieldVectors();
        FieldVector rowIdVector = vectors.getFirst();
        verify(rowIdVector.getNullCount() == 0,
                "%s cannot contain NULLs", rowIdVector.getName());

        final String VASTDB_ROWID_NAME = VASTDB_ROW_ID_FIELD.getName();
        Optional<Integer> toRemove = Optional.empty();
        for (int i = 0; i < vectors.size(); ++i) {
            Field field = vectors.get(i).getField();
            if (field.getName().equals(VASTDB_ROWID_NAME)) {
                LOG.debug("Dropping %s field due to ORION-158361", field);
                toRemove = Optional.of(i);
                break;
            }
        }
        return toRemove
                .map(index -> {
                    FieldVector vastDbRowIdVector = vectors.get(index);
                    verify(vastDbRowIdVector.getNullCount() == 0,
                            "%s cannot contain NULLs", vastDbRowIdVector.getName());

                    // It's OK to directly compare the data buffers, since there are no NULLs
                    ArrowBuf buf1 = rowIdVector.getDataBuffer();
                    ArrowBuf buf2 = vastDbRowIdVector.getDataBuffer();
                    if (ByteFunctionHelpers.compare(
                            buf1, 0, buf1.readableBytes(), buf2, 0, buf2.readableBytes()) == 0) {
                        // Workaround for ORION-158361 (drop `vastdb_rowid` column when doing updates)
                        return root.removeVector(index);
                    }
                    String msg = format("Cannot modify %s, table=%s batch=%s",
                            VASTDB_ROWID_NAME, mergeHandle.getTable(), root.contentToTSVString());
                    LOG.error(msg);
                    throw new TrinoException(READ_ONLY_VIOLATION, msg);
                })
                .orElse(root);
    }
}
