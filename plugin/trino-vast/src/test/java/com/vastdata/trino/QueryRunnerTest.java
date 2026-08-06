/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;

public abstract class QueryRunnerTest
        extends AbstractTestQueryFramework
{
    public static final String DEFAULT_SCHEMA_NAME = "testschema";
    public static final String DEFAULT_TABLE_NAME = "testtable";

    protected QueryRunner queryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return VastQueryRunner.createQueryRunner(
                Map.of("node.environment", "vast", "endpoint", System.getProperty("ENDPOINT", "http://localhost:9090")),
                "vast", 3, Map.of(), new TestingVastPlugin(getVastModule()));
    }

    protected abstract VastModule getVastModule();

    protected Session getSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession()).setSchema(DEFAULT_SCHEMA_NAME).build();
    }

    public record TestingVastPlugin(VastModule vastModule)
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return List.of(new VastConnectorFactory(vastModule));
        }
    }
}
