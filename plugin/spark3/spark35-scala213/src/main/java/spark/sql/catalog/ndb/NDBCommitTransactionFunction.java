/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.client.error.VastUserException;
import ndb.NDB;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class NDBCommitTransactionFunction extends AbstractNDBCommitTransactionFunction
        implements UnboundFunction, Serializable
{

    public NDBCommitTransactionFunction()
    {
        super();
        NDBTransactionFunctionsUtil.commit(NDB.alterTransaction, () -> {
            try {
                return NDB.getVastClient(NDB.getConfig());
            }
            catch (VastUserException e) {
                throw new RuntimeException(e);
            }
        });
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
                return getName();
            }
        };
    }

    @Override
    public String description()
    {
        return getDescription();
    }

    @Override
    public String name()
    {
        return getName();
    }
}
