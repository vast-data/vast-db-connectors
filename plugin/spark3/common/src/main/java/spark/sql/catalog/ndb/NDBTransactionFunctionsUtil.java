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
import scala.collection.mutable.HashMap;

import java.io.IOException;
import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public final class NDBTransactionFunctionsUtil
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(NDBTransactionFunctionsUtil.class);

    private NDBTransactionFunctionsUtil() {}

    public static void create(Supplier<HashMap<String, String>> envSupplier, Supplier<VastClient> vastClient) {
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient.get(), new VastSimpleTransactionFactory());
        HashMap<String, String> env = envSupplier.get();
        boolean contains = env.contains("tx");
        if (!contains) {
            SimpleVastTransaction simpleVastTransaction = transactionsManager.startTransaction(new StartTransactionContext(false, true));
            String tx_str = simpleVastTransaction.toString();
            LOG.info("creating tx={}", tx_str);
            env.put("tx", tx_str);
        }
        else {
            throw toRuntime(new VastUserException("Active transaction was found"));
        }
    }

    public static void commit(Supplier<HashMap<String, String>> envSupplier, Supplier<VastClient> vastClient)
    {
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient.get(), new VastSimpleTransactionFactory());
        HashMap<String, String> env = envSupplier.get();
        boolean contains = env.contains("tx");
        if (contains) {
            try {
                String tx_str = env.get("tx").get();
                LOG.info("commit t={}, env={}", tx_str, env);
                SimpleVastTransaction tx = OBJECT_MAPPER.readValue(tx_str, SimpleVastTransaction.class);
                transactionsManager.commit(tx);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            finally {
                env.remove("tx");
            }
        }
        else {
            throw toRuntime(new VastUserException("Failing commit as active transaction was not found"));
        }
    }

    public static void rollback(Supplier<HashMap<String, String>> envSupplier, Supplier<VastClient> client) {
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(client.get(), new VastSimpleTransactionFactory());
        HashMap<String, String> env = envSupplier.get();
        boolean contains = env.contains("tx");
        if (contains) {
            try {
                String tx_str = env.get("tx").get();
                LOG.info("rollback tx={}", tx_str);
                SimpleVastTransaction tx = OBJECT_MAPPER.readValue(env.get("tx").get(), SimpleVastTransaction.class);
                transactionsManager.rollback(tx);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            finally {
                env.remove("tx");
            }
        }
        else {
            throw toRuntime(new VastUserException("Failing rollback as active transaction was not found"));
        }
    }

}
