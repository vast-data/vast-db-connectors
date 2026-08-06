/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.ForVast;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastVersion;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.DataResponseParserMetrics;
import com.vastdata.client.metrics.MetricsDumper;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.metrics.VastMetrics;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.memory.MemoryLimiterMetrics;
import com.vastdata.memory.VastMemoryLimiter;
import com.vastdata.mockserver.VastMockS3ServerStarter;
import com.vastdata.trino.metrics.PageSinkMetrics;
import com.vastdata.trino.metrics.PageSourceMetrics;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tablefunction.VastTableFunction;
import com.vastdata.trino.tx.VastTransactionHandleFactory;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.function.FunctionProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.weakref.jmx.MBeanExporter;

import java.util.HashMap;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class VastModule
        implements Module
{
    private static final Logger LOG = Logger.get(VastModule.class);
    private final boolean isAccessControl;
    private final VastTrinoTransactionHandleManager transactionManagerInstance;
    private final VastPageSinkProvider pageSinkProviderInstance;
    private final VastClient vastClientInstance;
    private Map<Class<? extends VastMetrics<?>>, VastMetrics<?>> metrics;

    public VastModule(boolean isAccessControl,
                      VastTrinoTransactionHandleManager transactionManagerInstance,
                      VastPageSinkProvider pageSinkProviderInstance,
                      VastClient vastClientInstance)
    {
        this.isAccessControl = isAccessControl;
        this.transactionManagerInstance = transactionManagerInstance;
        this.pageSinkProviderInstance = pageSinkProviderInstance;
        this.vastClientInstance = vastClientInstance;
    }

    public static Builder builder(boolean isAccessControl)
    {
        return new Builder(isAccessControl);
    }

    @Provides
    @Singleton
    public RootAllocator provideRootAllocator()
    {
        LOG.info("Creating singleton RootAllocator for VAST connector");
        return new RootAllocator();
    }

    @Provides
    @Singleton
    public InsertBufferAllocator provideInsertBuffersAllocator(RootAllocator rootAllocator)
    {
        LOG.info("Creating insert buffers allocator for VAST connector");
        return new InsertBufferAllocator(
                rootAllocator.newChildAllocator("insert-buffers", 0,
                        Long.MAX_VALUE));
    }

    @Override
    public void configure(Binder binder)
    {
        VastMockS3ServerStarter.fromEnv();

        binder.bind(ClassLoader.class).toInstance(getClass().getClassLoader());

        LOG.info("Configuring VastModule sys_version=%s version_hash=%s",
                VastVersion.SYS_VERSION, VastVersion.HASH);
        httpClientBinder(binder)
                .bindHttpClient("vast", ForVast.class)
                .withConfigDefaults(
                        VastTrinoDependenciesFactory.HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS);
        newSetBinder(binder, SessionPropertiesProvider.class)
                .addBinding()
                .to(VastSessionProperties.class)
                .in(Scopes.SINGLETON);
        bindSingleton(binder, VastClient.class, vastClientInstance);
        bindSingleton(binder, VastTrinoTransactionHandleManager.class,
                transactionManagerInstance);
        bindSingleton(binder, VastPageSinkProvider.class,
                pageSinkProviderInstance);

        binder.bind(VastIoExecutor.class).in(Scopes.SINGLETON);
        binder.bind(VastCpuExecutor.class).in(Scopes.SINGLETON);
        binder.bind(VastQueryEngineClient.class).in(Scopes.SINGLETON);
        binder.bind(VastTransactionHandleFactory.class).in(Scopes.SINGLETON);
        binder
                .bind(VastDependenciesFactory.class)
                .to(VastTrinoDependenciesFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(VastSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(VastStatisticsManager.class).in(Scopes.SINGLETON);
        binder.bind(VastPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(VastAccessControl.class).in(Scopes.SINGLETON);
        binder.bind(VastMemoryLimiter.class).in(Scopes.SINGLETON);
        binder.bind(MetricsDumper.class).asEagerSingleton();
        binder.bind(ShapingLoggerFactory.class).asEagerSingleton();

        binder
                .bind(ConnectorNodePartitioningProvider.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(VastNodePartitioningProvider.class)
                .in(Scopes.SINGLETON);
        binder
                .bind(ConnectorNodePartitioningProvider.class)
                .to(ClassLoaderSafeNodePartitioningProvider.class)
                .in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(VastConfig.class);
        configBinder(binder).bindConfig(VastTrinoConfig.class);
        if (!isAccessControl) {
            binder.bind(VastConnector.class).in(Scopes.SINGLETON);
            binder
                    .bind(FunctionProvider.class)
                    .to(VastTableFunction.class)
                    .in(Scopes.SINGLETON);
        }

        this.metrics = bindMetrics();
        Multibinder<VastMetrics> metricsBinder = Multibinder.newSetBinder(
                binder, VastMetrics.class);
        metrics
                .values()
                .forEach(metric -> metricsBinder
                        .addBinding()
                        .toInstance(metric));
    }

    private <T> void bindSingleton(Binder binder, Class<T> clazz, T instance)
    {
        if (instance != null) {
            binder.bind(clazz).toInstance(instance);
        }
        else {
            binder.bind(clazz).in(Scopes.SINGLETON);
        }
    }

    private Map<Class<? extends VastMetrics<?>>, VastMetrics<?>> bindMetrics()
    {
        Map<Class<? extends VastMetrics<?>>, VastMetrics<?>> ret = new HashMap<>();
        ret.put(DataResponseParserMetrics.class, new DataResponseParserMetrics());
        ret.put(PageSourceMetrics.class, new PageSourceMetrics());
        ret.put(SplitSourceMetrics.class, new SplitSourceMetrics());
        ret.put(RecordBatchSplitterMetrics.class, new RecordBatchSplitterMetrics());
        ret.put(BufferedInsertMetrics.class, new BufferedInsertMetrics());
        ret.put(ByColumnInserterMetrics.class, new ByColumnInserterMetrics());
        ret.put(PageSinkMetrics.class, new PageSinkMetrics());
        ret.put(MemoryLimiterMetrics.class, new MemoryLimiterMetrics());

        return ret;
    }

    @Provides
    public DataResponseParserMetrics dataResponseMetrics(MBeanExporter exporter)
    {
        DataResponseParserMetrics metrics = (DataResponseParserMetrics) this.metrics.get(
                DataResponseParserMetrics.class);
        exporter.export("com.vastdata.trino:type=DataResponseMetrics", metrics);
        return metrics;
    }

    @Provides
    public PageSinkMetrics pageSinkMetrics(MBeanExporter exporter)
    {
        PageSinkMetrics metrics = (PageSinkMetrics) this.metrics.get(
                PageSinkMetrics.class);
        exporter.export("com.vastdata.trino:type=PageSinkMetrics", metrics);
        return metrics;
    }

    @Provides
    public MemoryLimiterMetrics pageMemoryLimiterMetrics(MBeanExporter exporter)
    {
        MemoryLimiterMetrics metrics = (MemoryLimiterMetrics) this.metrics.get(
                MemoryLimiterMetrics.class);
        exporter.export("com.vastdata.trino:type=MemoryLimiterMetrics", metrics);
        return metrics;
    }

    @Provides
    public SplitSourceMetrics splitSourceMetrics(MBeanExporter exporter)
    {
        SplitSourceMetrics metrics = (SplitSourceMetrics) this.metrics.get(
                SplitSourceMetrics.class);
        exporter.export("com.vastdata.trino:type=SplitSourceMetrics", metrics);
        return metrics;
    }

    @Provides
    public RecordBatchSplitterMetrics recordBatchSplitterMetrics(MBeanExporter exporter)
    {
        RecordBatchSplitterMetrics metrics = (RecordBatchSplitterMetrics) this.metrics.get(
                RecordBatchSplitterMetrics.class);
        exporter.export("com.vastdata.trino:type=RecordBatchSplitterMetrics",
                metrics);
        return metrics;
    }

    @Provides
    public BufferedInsertMetrics bufferedInsertMetrics(MBeanExporter exporter)
    {
        BufferedInsertMetrics metrics = (BufferedInsertMetrics) this.metrics.get(
                BufferedInsertMetrics.class);
        exporter.export("com.vastdata.trino:type=BufferedInsertMetrics",
                metrics);
        return metrics;
    }

    @Provides
    public ByColumnInserterMetrics byColumnInserterMetrics(MBeanExporter exporter)
    {
        ByColumnInserterMetrics metrics = (ByColumnInserterMetrics) this.metrics.get(
                ByColumnInserterMetrics.class);
        exporter.export("com.vastdata.trino:type=ByColumnInserterMetrics",
                metrics);
        return metrics;
    }

    @Provides
    public PageSourceMetrics pageSourceMetrics(MBeanExporter exporter)
    {
        PageSourceMetrics metrics = (PageSourceMetrics) this.metrics.get(
                PageSourceMetrics.class);
        exporter.export("com.vastdata.trino:type=PageSourceMetrics", metrics);
        return metrics;
    }

    public record InsertBufferAllocator(BufferAllocator allocator)
    {}

    public static class Builder
    {
        private final boolean isAccessControl;
        private VastTrinoTransactionHandleManager transactionManagerInstance;
        private VastPageSinkProvider pageSinkProviderInstance;
        private VastClient vastClientInstance;

        private Builder(boolean isAccessControl)
        {
            this.isAccessControl = isAccessControl;
        }

        public Builder withTransactionManager(VastTrinoTransactionHandleManager transactionManager)
        {
            this.transactionManagerInstance = transactionManager;
            return this;
        }

        public Builder withPageSinkProvider(VastPageSinkProvider pageSinkProvider)
        {
            this.pageSinkProviderInstance = pageSinkProvider;
            return this;
        }

        public Builder withVastClient(VastClient vastClient)
        {
            this.vastClientInstance = vastClient;
            return this;
        }

        public VastModule build()
        {
            return new VastModule(isAccessControl, transactionManagerInstance,
                    pageSinkProviderInstance, vastClientInstance);
        }
    }
}
