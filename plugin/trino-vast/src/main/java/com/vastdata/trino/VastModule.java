/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.vastdata.client.ForVast;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.mockserver.VastMockS3ServerStarter;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandleFactory;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.session.SessionPropertiesProvider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class VastModule
        implements Module
{
    private static final Logger LOG = Logger.get(VastModule.class);

    @Override
    public void configure(Binder binder)
    {
        VastMockS3ServerStarter.fromEnv();
        LOG.info("Configuring VastModule");
        httpClientBinder(binder).bindHttpClient("vast", ForVast.class)
                .withConfigDefaults(VastTrinoDependenciesFactory.HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(VastSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(VastClient.class).in(Scopes.SINGLETON);
        binder.bind(VastTransactionHandleFactory.class).in(Scopes.SINGLETON);
        binder.bind(VastTrinoTransactionHandleManager.class).in(Scopes.SINGLETON);
        binder.bind(VastConnector.class).in(Scopes.SINGLETON);
        binder.bind(VastDependenciesFactory.class).to(VastTrinoDependenciesFactory.class).in(Scopes.SINGLETON);
        binder.bind(VastSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(VastStatisticsManager.class).in(Scopes.SINGLETON);
        binder.bind(VastPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(VastPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(VastSecurityAccessControl.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(VastConfig.class);
    }
}
