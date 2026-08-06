/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.base.Predicates;
import com.vastdata.client.ParsedURL;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vastdata.client.ParsedURL.PATH_SEPERATOR;
import static com.vastdata.client.ParsedURL.compose;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static java.util.Objects.requireNonNull;

public class VastCatalogUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastCatalogUtils.class);

    private final VastConfig config;
    private final VastClient vastClient;
    private final VastTransactionHandleManager<SimpleVastTransaction> transactionsManager;

    public VastCatalogUtils(VastConfig config, VastClient vastClient,
            VastTransactionHandleManager<SimpleVastTransaction> transactionsManager)
    {
        this.config = requireNonNull(config);
        this.vastClient = requireNonNull(vastClient);
        this.transactionsManager = requireNonNull(transactionsManager);
    }

    public static List<String> getSortedByColumns(String value)
    {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }
        return Stream.of(value.split(",")).map(String::trim).collect(
                Collectors.toList());
    }

    /**
     * should be replaced with a proper config getter
     *
     * @return VastConfig
     */
    public VastConfig getConfig()
    {
        return config;
    }

    public String[][] listNamespaces(String[] namespace, int pageSize,
            VastTransactionHandleManager<SimpleVastTransaction> transactionsManager,
            String endUser)
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
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            return vastClient.listAllSchemas(tx, pageSize, endUser).filter(
                    stringPredicate).map(s -> {
                String[] rawParts = ParsedURL.parse(s);
                String[] parts = Arrays.copyOfRange(rawParts, 1,
                        rawParts.length);
                LOG.debug("listNamespaces adding to results: {}",
                        Arrays.toString(parts));
                return parts;
            }).toArray(String[][]::new);
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    public RowColumnSecurityResponse getRowColumnSecurity(String schema,
            String table, String endUser)
    {
        LOG.debug(
                "Utils getRowColumnSecurity: schema={}, table={}, user={}, securityEnable={}",
                schema, table, endUser, config.isRowColumnSecurityEnabled());
        if (!config.isRowColumnSecurityEnabled()) {
            LOG.warn("Row column security is disabled");
            return null;
        }
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            LOG.debug("Utils getRowColumnSecurity: schema={}, table={}, tx={}",
                    schema, table, tx);
            String endUserForFetching = config.isEndUserImpersonationEnabled() ?
                    endUser :
                    null;
            return vastClient.getRowColumnSecurity(tx, schema, table,
                    endUserForFetching);
        }
        catch (VastUserException | VastServerException e) {
            LOG.warn("failed getRowColumnSecurity: schema={}, table={}", schema,
                    table, e);
            return null; // TODO - rollback
        }
    }

    public void checkScanIsAllowed(String tableName,
                                   boolean hasPostFilter,
                                   boolean hasPusedPredicates)
            throws VastRuntimeException
    {
        boolean isPit = tableName.contains(PIT_NAME_SUFFIX);
        boolean hasRowFilter = hasPostFilter || hasPusedPredicates;
        if (isPit && hasRowFilter) {
            throw new VastRuntimeException("Access Denied", null,
                    ErrorType.USER);
        }

    }
}
