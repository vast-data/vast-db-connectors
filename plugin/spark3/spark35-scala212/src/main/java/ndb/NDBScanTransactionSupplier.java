/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class NDBScanTransactionSupplier
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBScanTransactionSupplier.class);
    public static final SparkQueryIDSupplier SPARK_QUERY_ID_SUPPLIER = new SparkQueryIDSupplier();
    public static final VastSparkTransactionsManager transactionsManager;
    static {
        try {
            transactionsManager = VastSparkTransactionsManager.getInstance(NDB.getVastClient(NDB.getConfig()), new VastTransactionFactory());
        }
        catch (VastUserException e) {
            throw toRuntime("Failed creating a new transaction", e);
        }
    }

    private NDBScanTransactionSupplier() {}

    public static SimpleVastTransaction supplyTransaction()
    {
        SimpleVastTransaction existing = VastAutocommitTransaction.getExisting();
        if  (existing != null) {
            return existing;
        }
        else {
            SimpleVastTransaction newTx = transactionsManager.startTransaction(null);
            LOG.info("supplyTransaction() created new transaction: {}", newTx);
            NDB.getTxRegistry().registerTransaction(SPARK_QUERY_ID_SUPPLIER.getAsLong(), newTx);
            return newTx;
        }
    }
}
