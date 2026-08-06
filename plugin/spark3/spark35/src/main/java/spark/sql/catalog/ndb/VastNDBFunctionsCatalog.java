/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

class VastNDBFunctionsCatalog
        implements FunctionCatalog
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastNDBFunctionsCatalog.class);

    private final Map<String, Supplier<UnboundFunction>> functions = new HashMap<>(
            3);

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options)
    {
        functions.put(NDBFunction.CREATE_TX.getFuncName(),
                NDBFunctionFactory.getFor(NDBFunction.CREATE_TX));
        functions.put(NDBFunction.COMMIT_TX.getFuncName(),
                NDBFunctionFactory.getFor(NDBFunction.COMMIT_TX));
        functions.put(NDBFunction.ROLLBACK_TX.getFuncName(),
                NDBFunctionFactory.getFor(NDBFunction.ROLLBACK_TX));
        functions.put("years", YearsFunction::new);
        functions.put("months", MonthsFunction::new);
        functions.put("days", DaysFunction::new);
        functions.put("hours", HoursFunction::new);
        functions.put("bucket", BucketFunction::new);
    }

    @Override
    public Identifier[] listFunctions(String[] namespace)
    {
        return functions.keySet().stream().map(
                ndbFunc -> Identifier.of(namespace, ndbFunc)).toArray(
                Identifier[]::new);
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident)
            throws NoSuchFunctionException
    {
        LOG.info("loadFunction: {}", ident);
        Supplier<UnboundFunction> function = functions.get(
                ident.name().toLowerCase(Locale.getDefault()));
        if (function != null) {
            return function.get();
        }
        else if (ident.name().toLowerCase(Locale.getDefault()).startsWith(
                "truncate")) {
            int width = Integer.parseInt(
                    ident.name().substring("truncate_".length()));
            return new BoundTruncateFunction(width);
        }
        else {
            throw new NoSuchFunctionException(ident);
        }
    }

    @Override
    public boolean functionExists(Identifier ident)
    {
        return functions.containsKey(ident.name());
    }

    // never called
    @Override
    public String name()
    {
        return null;
    }
}
