/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public final class NDBTransactionFunctionsUtil
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(NDBTransactionFunctionsUtil.class);

    private NDBTransactionFunctionsUtil() {}

    public static void create(BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction, Supplier<VastClient> vastClient) {
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient.get(), new VastSimpleTransactionFactory());
        alterTransaction.accept(false, maybeTransaction -> {
            if (!maybeTransaction.isPresent()) {
                SimpleVastTransaction simpleVastTransaction = transactionsManager.startTransaction(new StartTransactionContext(false, true));
                String tx_str = simpleVastTransaction.toString();
                LOG.info("creating tx={}", tx_str);
                return Optional.of(tx_str);
            }
            else {
                throw toRuntime(new VastUserException("Active transaction was found"));
            }
        });
    }

    public static void commit(BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction, Supplier<VastClient> vastClient)
    {
        final VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient.get(), new VastSimpleTransactionFactory());
        alterTransaction.accept(true, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                try {
                    String tx_str = maybeTransaction.get();
                    LOG.info("commit t={}, env={}", tx_str, alterTransaction);
                    SimpleVastTransaction tx = OBJECT_MAPPER.readValue(tx_str, SimpleVastTransaction.class);
                    transactionsManager.commit(tx);
                    return Optional.empty();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                throw toRuntime(new VastUserException("Failing commit as active transaction was not found"));
            }
        });
    }

    public static void rollback(BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction, Supplier<VastClient> client) {
        final VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(client.get(), new VastSimpleTransactionFactory());
        alterTransaction.accept(true, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                try {
                    String tx_str = maybeTransaction.get();
                    LOG.info("rollback tx={}", tx_str);
                    SimpleVastTransaction tx = OBJECT_MAPPER.readValue(tx_str, SimpleVastTransaction.class);
                    transactionsManager.rollback(tx);
                    return Optional.empty();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                throw toRuntime(new VastUserException("Failing rollback as active transaction was not found"));
            }
        });
    }
}
