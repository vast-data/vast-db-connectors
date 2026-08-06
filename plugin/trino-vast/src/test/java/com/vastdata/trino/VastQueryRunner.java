/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;

import java.util.Map;

public class VastQueryRunner
{
    private static final Logger logger = Logger.get(VastQueryRunner.class);

    private VastQueryRunner()
    {
    }

    public static QueryRunner createQueryRunner(Map<String, String> vastCatalogConfig,
                                                String catalogName,
                                                int numOfNodes,
                                                Map<String, String> coordinatorProperties)
            throws Exception
    {
        return createQueryRunner(vastCatalogConfig, catalogName, numOfNodes, coordinatorProperties,
                (Plugin) new VastPlugin());
    }

    public static QueryRunner createQueryRunner(
            Map<String, String> vastCatalogConfig, String catalogName,
            int numOfNodes, Map<String, String> coordinatorProperties, Plugin plugin)
            throws Exception
    {
        DistributedQueryRunner.Builder<?> queryRunnerBuilder = DistributedQueryRunner
                .builder(createSession(catalogName))
                .setWorkerCount(numOfNodes - 1)
                .setCoordinatorProperties(coordinatorProperties);
        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();

        try {
            queryRunner.installPlugin(plugin);
            queryRunner.createCatalog(catalogName, "vast", vastCatalogConfig);
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            logger.error(e, "failed creating query runner");
            throw e;
        }
    }

    private static Session createSession(String catalogName)
    {
        return TestingSession
                .testSessionBuilder()
                .setCatalog(catalogName)
                .setSystemProperty(SystemSessionProperties.REDISTRIBUTE_WRITES,
                        "true")
                .build();
    }
}
