/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastTooLargePageException;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.UpdatablePageSource;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerDelete;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerUpdate;
import static java.util.Objects.requireNonNull;

// Used to wrap an existing VastPageSource, to support DELETE & UPDATE queries
// see https://trino.io/docs/current/develop/delete-and-update.html
public class VastUpdatablePageSource
        implements UpdatablePageSource
{
    private static final Logger LOG = Logger.get(VastUpdatablePageSource.class);

    private final VastPageSource source;
    private final VastClient client;
    private final VastTransaction tx;
    private final VastTableHandle table;
    private final AtomicReference<URI> dataEndpoint; // will be set by VastClient#queryData
    private final int maxRowsPerUpdate;
    private final int maxRowsPerDelete;

    public VastUpdatablePageSource(VastPageSource source, VastClient client, VastTransaction tx, VastTableHandle table, AtomicReference<URI> dataEndpoint, ConnectorSession session)
    {
        this.source = requireNonNull(source, "source is null");
        this.client = requireNonNull(client, "client is null");
        this.tx = requireNonNull(tx, "tx is null");
        this.table = requireNonNull(table, "table is null");
        this.dataEndpoint = requireNonNull(dataEndpoint, "dataEndpoint is null");
        this.maxRowsPerUpdate = getMaxRowsPerUpdate(session);
        this.maxRowsPerDelete = getMaxRowsPerDelete(session);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public long getCompletedBytes()
    {
        return source.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return source.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return source.getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return source.getMemoryUsage();
    }

    @Override
    public void close()
    {
        source.close();
    }

    private static VastRecordBatchBuilder recordBatchBuilder(VastTableHandle table)
    {
        verify(table.getUpdatable(), "%s is not updatable", table);
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.add(ROW_ID_FIELD); // row ID column is expected to be the first one
        table.getUpdatedColumns().stream().map(VastColumnHandle::getField).forEach(fields::add);
        return new VastRecordBatchBuilder(new Schema(fields.build()));
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        LOG.debug("deleteRows(%d IDs, %d bytes)", rowIds.getPositionCount(), rowIds.getSizeInBytes());
        try (VectorSchemaRoot root = recordBatchBuilder(table).build(new Page(rowIds))) {
            client.deleteRows(tx, table.getSchemaName(), table.getTableName(), root, dataEndpoint.get(), Optional.of(maxRowsPerDelete));
        }
        catch (VastTooLargePageException e) {
            throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        LOG.debug("updateRows(%d IDs, %d bytes, %s columns)", page.getPositionCount(), page.getSizeInBytes(), columnValueAndRowIdChannels);
        int channels = columnValueAndRowIdChannels.size();
        Block[] blocks = new Block[channels];

        int rowIdChannel = columnValueAndRowIdChannels.get(channels - 1); // last channel contains row IDs
        blocks[0] = page.getBlock(rowIdChannel);
        for (int i = 0; i < channels - 1; ++i) {
            int dataChannel = columnValueAndRowIdChannels.get(i);
            blocks[i + 1] = page.getBlock(dataChannel);
        }
        LOG.debug("reordered %d blocks: %s", blocks.length, Arrays.toString(blocks));
        try (VectorSchemaRoot root = recordBatchBuilder(table).build(new Page(blocks))) {
            client.updateRows(tx, table.getSchemaName(), table.getTableName(), root, dataEndpoint.get(), Optional.of(maxRowsPerUpdate));
        }
        catch (VastTooLargePageException e) {
            throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void abort()
    {
        LOG.warn("aborting");
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return UpdatablePageSource.super.isBlocked();
    }
}
