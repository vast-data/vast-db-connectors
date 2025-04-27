package com.vastdata.spark;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.VastVersion;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.function.Predicate;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class VastSparkCommonDependenciesFactory
    implements VastDependenciesFactory
{
    @VisibleForTesting protected static final String VAST_SPARK_CLIENT_TAG = "VastSparkPlugin/" + VastVersion.SYS_VERSION;
    public static final String VAST_SPARK_PLUGIN_V_1 = "VastSparkPlugin.v1";
    ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS = cfg -> {
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

    private final VastConfig vastConfig;
    private final Predicate<String> schemaNamePredicate = new ValidSchemaNamePredicate();

    protected VastSparkCommonDependenciesFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
    }

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory()
    {
        return new CommonRequestHeadersBuilder(() -> VAST_SPARK_CLIENT_TAG + "-spark-" + vastConfig.getEngineVersion());
    }

    @Override
    public String getConnectorVersionedStatisticsTag()
    {
        return VAST_SPARK_PLUGIN_V_1;
    }

    @Override
    public ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults()
    {
        return HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS;
    }
}
