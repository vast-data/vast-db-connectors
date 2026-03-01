/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.vastdata.client.tx.VastTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Consumer;

public class NDBTransactionsRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBTransactionsRegistry.class);

    private final Multimap<Long, VastTransaction> transactionsPerQueryId = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
    Consumer<VastTransaction> transactionConsumer;

    NDBTransactionsRegistry(Consumer<VastTransaction> transactionConsumer)
    {
        this.transactionConsumer =  transactionConsumer;
    }

    public void registerTransaction(long id, VastTransaction transaction)
    {
        LOG.info("Registering transaction: {} for id: {}", transaction, id);
        transactionsPerQueryId.put(id, transaction);
    }

    public void closeTransactions(long id)
    {
        Collection<VastTransaction> vastTransactions = transactionsPerQueryId.removeAll(id);
        LOG.debug("Committing transactions for id: {}: {}", id, vastTransactions);
        vastTransactions.forEach(transactionConsumer);
    }
}
