/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.error.SparkExceptionFactory;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public abstract class NDBCommon
{
    protected static Logger LOG = null;
    public static final String TRANSACTION_KEY = "tx";

    protected static final AtomicBoolean isInitialized = new AtomicBoolean(false);
    public static BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction = (cancelOnFailure, f) -> {
        throw new IllegalStateException("Env supplier is unset");
    };
    protected static Supplier<VastConfig> vastConfigSupplier = null;
    protected static Function<VastConfig, VastDependenciesFactory> dependencyFactoryFunction = null;
    protected static Runnable initRoutine = null;
    protected static VastConfig config = null;
    protected static HttpClient httpClient = null;
    protected static VastClient vastClient = null;
    protected static VastDependenciesFactory dependenciesFactory;

    protected static void clearConfig()
    {
        isInitialized.set(false);
        config = null;
        httpClient = null;
    }

    protected static void setCommonConfig(VastConfig vastConfig, Function<VastConfig, VastDependenciesFactory> dependencyFactoryFunction)
    {
        HttpClientConfig httpConfig = new HttpClientConfig();
        dependenciesFactory = dependencyFactoryFunction.apply(vastConfig);
        dependenciesFactory.getHttpClientConfigConfigDefaults().setDefaults(httpConfig);
        HttpClient tmpHttpClient = new JettyHttpClient("ndb", httpConfig);
        config = vastConfig;
        httpClient = tmpHttpClient;
    }

    // called using ndb.NDB.init(spark)
    protected static void init()
    {
        if (!isInitialized.get()) {
            initCommonConfig(vastConfigSupplier.get());
        }
        else {
            LOG.warn("Module already initialized");
        }
    }

    protected static synchronized void initCommonConfig(VastConfig vastConfig)
    {
        if (!isInitialized.get()) {
            VastAutocommitTransaction.alterTransaction = alterTransaction;
            setCommonConfig(vastConfig, dependencyFactoryFunction);
            isInitialized.set(true);
        }
        else {
            LOG.warn("Module already configured");
        }
    }

    protected static synchronized VastConfig getConfig()
            throws VastUserException
    {
        if (config == null) {
            initRoutine.run();
            if (config == null) {
                throw SparkExceptionFactory.uninitializedConfig();
            }
            else {
                return config;
            }
        }
        else {
            return config;
        }
    }

    protected static synchronized HttpClient getHTTPClient()
            throws VastUserException
    {
        if (httpClient == null) {
            initRoutine.run();
            if (httpClient == null) {
                throw SparkExceptionFactory.uninitializedConfig();
            }
            else {
                return httpClient;
            }
        }
        else {
            return httpClient;
        }
    }

    protected static VastClient getVastClient(VastConfig vastConfig)
            throws VastUserException
    {
        if (vastClient == null) {
            createVastClient(vastConfig);
        }
        return vastClient;
    }

    private static synchronized void createVastClient(VastConfig vastConfig)
            throws VastUserException
    {
        if (vastClient == null) {
            initCommonConfig(vastConfig);
            vastClient = new VastClient(getHTTPClient(), vastConfig, dependencyFactoryFunction.apply(vastConfig));
        }
    }

    protected static VastDependenciesFactory getSparkDependenciesFactory()
    {
        if (dependenciesFactory == null) {
            return dependencyFactoryFunction.apply(null);
        }
        else {
            return dependenciesFactory;
        }
    }

    protected static void logConfEntry(String key, String vastEndpoint)
    {
        LOG.info("NDB init conf: {}, {}", key, vastEndpoint);
    }
}
