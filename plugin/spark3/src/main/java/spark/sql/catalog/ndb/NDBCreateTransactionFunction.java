/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.HashMap;

import java.io.Serializable;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class NDBCreateTransactionFunction
        implements UnboundFunction, Serializable
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBCreateTransactionFunction.class);

    public NDBCreateTransactionFunction(VastConfig conf)
    {
        VastSparkTransactionsManager transactionsManager;
        try {
            transactionsManager = VastSparkTransactionsManager.getInstance(NDB.getVastClient(conf), new VastSimpleTransactionFactory());
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();
        HashMap<String, String> env = sparkContext.executorEnvs();
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

    @Override
    public BoundFunction bind(StructType inputType)
    {
        return new ScalarFunction<Boolean>()
        {
            @Override
            public Boolean produceResult(InternalRow input)
            {
                return true;
            }

            @Override
            public DataType[] inputTypes()
            {
                return new DataType[0];
            }

            @Override
            public DataType resultType()
            {
                return DataTypes.BooleanType;
            }

            @Override
            public String name()
            {
                return NDBFunction.CREATE_TX.getFuncName();
            }
        };
    }

    @Override
    public String description()
    {
        return "Creating Vast transaction";
    }

    @Override
    public String name()
    {
        return NDBFunction.CREATE_TX.getFuncName();
    }
}
