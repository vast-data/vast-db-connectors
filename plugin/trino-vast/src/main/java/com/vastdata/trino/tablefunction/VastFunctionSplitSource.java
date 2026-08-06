/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.vastdata.client.queryengine.ServerQueryState;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.arrow.memory.RootAllocator;
import vastdb.queryengine.protocol.GetQueryStatusResponse;
import vastdb.queryengine.protocol.QueryId;
import vastdb.queryengine.protocol.StartQueryResponse;
import vastdb.queryengine.protocol.Ticket;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

import static com.vastdata.trino.tablefunction.VastConnectorTableFunctionHandle.GROUPS_KEYWORD;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class VastFunctionSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(VastFunctionSplitSource.class);

    private final VastQueryEngineClient vastQEClient;
    private final VastTrinoTransactionHandleManager vastTransactionHandleManager;
    private final VastTransactionHandle queryTx;
    private final VastConnectorTableFunctionHandle functionHandle;
    private final ConnectorSession session;
    private final ArrowSchemaUtils arrowSchemaUtils;
    private ServerQueryState queryState = ServerQueryState.Init;
    private Optional<QueryId> internalQueryId = Optional.empty();
    private byte[] schema;
    private int lastTicket;

    public VastFunctionSplitSource(VastQueryEngineClient vastQEClient,
                                   VastTrinoTransactionHandleManager vastTransactionHandleManager,
                                   VastTransactionHandle transaction,
                                   VastConnectorTableFunctionHandle functionHandle,
                                   ConnectorSession session)
    {
        this(vastQEClient, vastTransactionHandleManager, transaction,
                functionHandle, session, new ArrowSchemaUtils());
    }

    @VisibleForTesting
    VastFunctionSplitSource(VastQueryEngineClient vastQEClient,
                            VastTrinoTransactionHandleManager vastTransactionHandleManager,
                            VastTransactionHandle transaction,
                            VastConnectorTableFunctionHandle functionHandle,
                            ConnectorSession session,
                            ArrowSchemaUtils arrowSchemaUtils)
    {
        this.vastQEClient = vastQEClient;
        this.queryTx = transaction;
        this.vastTransactionHandleManager = vastTransactionHandleManager;
        this.functionHandle = functionHandle;
        this.session = session;
        this.arrowSchemaUtils = arrowSchemaUtils;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (internalQueryId.isEmpty()) {
            try (RootAllocator allocator = new RootAllocator()) {
                String query = functionHandle.enforceIdentity() ?
                        replaceGroupsPlaceHolder() :
                        functionHandle.query();
                StartQueryResponse response = vastQEClient.startQuery(queryTx,
                        query);
                this.schema = arrowSchemaUtils
                        .parseSchema(response.getArrowSchema().toByteArray(),
                                allocator)
                        .serializeAsMessage();
                this.internalQueryId = Optional.of(
                        response.getStatus().getQueryId());
                this.vastTransactionHandleManager.registerQEQuery(queryTx,
                        internalQueryId.orElseThrow());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        List<ConnectorSplit> nextSplits = getNextTickets();
        return completedFuture(
                new ConnectorSplitBatch(nextSplits, isFinished()));
    }

    private String replaceGroupsPlaceHolder()
    {
        StringJoiner groupsJoiner = new StringJoiner(
                ","); // assuming groups are varchar and should be wrapped with ''
        session.getIdentity().getGroups().forEach((group) ->
        {
            group = "'" + group + "'";
            groupsJoiner.add(group);
        });
        return functionHandle
                .query()
                .replaceAll(GROUPS_KEYWORD, groupsJoiner.toString());
    }

    private List<ConnectorSplit> getNextTickets()
    {
        ImmutableList.Builder<ConnectorSplit> splitsBuilder = ImmutableList.builder();
        try {
            GetQueryStatusResponse statusResponse = vastQEClient.getQueryStatus(
                    internalQueryId.orElseThrow());
            List<Ticket> tickets = statusResponse.getStatus().getTicketsList();
            List<Ticket> newTickets = tickets.subList(lastTicket,
                    tickets.size());
            newTickets.forEach((ticket -> splitsBuilder.add(
                    new VastTableFunctionSplit(
                            internalQueryId.orElseThrow().toByteArray(),
                            ticket.toByteArray(), schema))));
            lastTicket = tickets.size();
            LOG.debug("got %d new tickets", newTickets.size());
            queryState = ServerQueryState.fromIpcValue(
                    statusResponse.getStatus().getState());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return splitsBuilder.build();
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return queryState == ServerQueryState.Completed || queryState == ServerQueryState.Invalid;
    }
}
