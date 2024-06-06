/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.base.Predicates;
import com.vastdata.client.ParsedURL;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Predicate;

import static com.vastdata.client.ParsedURL.PATH_SEPERATOR;
import static com.vastdata.client.ParsedURL.compose;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public final class VastCatalogUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(VastCatalogUtils.class);

    private VastCatalogUtils() {}

    public static String[][] listNamespaces(VastClient vastClient, String[] namespace,
            int pageSize, VastTransactionHandleManager<SimpleVastTransaction> transactionsManager)
    {
        Predicate<String> stringPredicate;
        if (namespace.length > 0) {
            String name = compose(namespace) + PATH_SEPERATOR;
            LOG.debug("listNamespaces starting with name: {}", name);
            stringPredicate = schema -> schema.startsWith(name);
        }
        else {
            LOG.debug("listNamespaces ALL");
            stringPredicate = Predicates.alwaysTrue();
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            return vastClient.listAllSchemas(tx, pageSize).filter(stringPredicate).map(s -> {
                String[] rawParts = ParsedURL.parse(s);
                String[] parts = Arrays.copyOfRange(rawParts, 1, rawParts.length);
                LOG.debug("listNamespaces adding to results: {}", Arrays.toString(parts));
                return parts;
            }).toArray(String[][]::new);
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }
}
