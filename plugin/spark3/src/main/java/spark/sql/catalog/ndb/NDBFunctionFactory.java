/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.client.VastConfig;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

import java.util.EnumMap;
import java.util.function.Function;

public final class NDBFunctionFactory
{
    private static final EnumMap<NDBFunction, Function<VastConfig, UnboundFunction>> implMap = new EnumMap<>(NDBFunction.class);

    static {
        implMap.put(NDBFunction.CREATE_TX, NDBCreateTransactionFunction::new);
        implMap.put(NDBFunction.COMMIT_TX, NDBCommitTransactionFunction::new);
        implMap.put(NDBFunction.ROLLBACK_TX, NDBRollbackTransactionFunction::new);
    }

    private NDBFunctionFactory() {}

    public static Function<VastConfig, UnboundFunction> getFor(NDBFunction function)
    {
        return implMap.get(function);
    }
}
