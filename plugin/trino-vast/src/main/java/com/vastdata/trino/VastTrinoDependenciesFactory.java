/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.VastVersion;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.connector.Connector;

import java.util.function.Predicate;

import static com.vastdata.client.RequestsHeaders.END_USER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class VastTrinoDependenciesFactory
        implements VastDependenciesFactory
{
    private static final Logger LOG = Logger.get(VastTrinoDependenciesFactory.class);

    static final ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS = cfg -> {
        cfg.setConnectTimeout(new Duration(1, MINUTES));
        cfg.setIdleTimeout(new Duration(3600, SECONDS));
        cfg.setRequestTimeout(new Duration(360000, SECONDS));
        cfg.setMaxConnectionsPerServer(250);
        cfg.setMaxContentLength(DataSize.of(32, MEGABYTE));
        cfg.setSelectorCount(10);
        cfg.setTimeoutThreads(8);
        cfg.setTimeoutConcurrency(4);
        cfg.setKeyStorePath(null); // explicit overwrite the keyStorePath (used by jetty sslContextFactory)
    };

    @VisibleForTesting protected static final String VAST_TRINO_CLIENT_TAG = "VastTrinoPlugin-" + VastVersion.SYS_VERSION;
    private final Predicate<String> schemaNamePredicate = new ValidSchemaNamePredicate();

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory(final String endUser)
    {
        String trinoVersion = Connector.class.getPackage().getImplementationVersion();
        final VastRequestHeadersBuilder builder = new CommonRequestHeadersBuilder(() -> VAST_TRINO_CLIENT_TAG + "-trino-" + trinoVersion);
        if (endUser != null) {
            if (VastSecurityAccessControl.isEndUserImpersonationEnabled()) {
                LOG.debug("end-user-impersonation is enabled, adding header %s=%s", END_USER.getHeaderName(), endUser);
                return builder.withEndUser(endUser);
            }
            LOG.warn("end-user-impersonation is disabled, omitting header %s=%s", END_USER.getHeaderName(), endUser);
        }
        return builder;
    }

    @Override
    public ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults()
    {
        return HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS;
    }

    @Override
    public final String getConnectorVersionedStatisticsTag() {
        String trinoVersion = "v1";
        return "VastTrinoPlugin." + trinoVersion;
    }

    @Override
    public StatisticsUrlExtractor<VastTableHandle> getStatisticsUrlHelper()
    {
        return TrinoStatisticsUrlExtractor.instance();
    }
}
