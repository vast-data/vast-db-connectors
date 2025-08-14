/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestVastSmokeIT
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "build";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return VastQueryRunner.createQueryRunner(
                Map.of("node.environment", "vast",
                        "endpoint", System.getProperty("ENDPOINT", "http://localhost:9090"),
                        "access_key_id", System.getProperty("AWS_ACCESS_KEY_ID", "your access key id"),
                        "secret_access_key", System.getProperty("AWS_SECRET_ACCESS_KEY", "your secret access key")),
                "vast",
                1,
                Map.of());
    }

    @BeforeAll
    void checkPrecondition()
    {
        assumeTrue(System.getenv("INTEG_TEST") != null, "Skipping all tests: INTEG_TEST not set");
    }

    @BeforeEach
    public void setUp()
    {
        computeActual(format("CREATE SCHEMA IF NOT EXISTS \"%s/s1\"", BUCKET_NAME));
        computeActual(format("CREATE TABLE IF NOT EXISTS \"%s/s1\".t (c1 INTEGER, c2 VARCHAR)", BUCKET_NAME));
    }

    @AfterEach
    public void tearDown()
    {
        computeActual(format("DROP TABLE \"%s/s1\".t", BUCKET_NAME));
        computeActual(format("DROP SCHEMA \"%s/s1\"", BUCKET_NAME));
    }

    @Test
    public void testSimpleWithoutWarmReturnProxy()
    {
        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (1, 'shlomi')", BUCKET_NAME));
        MaterializedResult materializedRows = computeActual(format("SELECT c1 FROM \"%s/s1\".t WHERE c1 = 1", BUCKET_NAME));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testAnalyze()
    {
        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (1, 'shlomi')", BUCKET_NAME));
        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (2, 'alfasi')", BUCKET_NAME));
        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (3, 'shlomi')", BUCKET_NAME));
        MaterializedResult materializedRows = computeActual(format("SELECT c1 FROM \"%s/s1\".t WHERE c1 = 1", BUCKET_NAME));
        computeActual(format("ANALYZE \"%s/s1\".t WITH (columns = ARRAY['c1'])", BUCKET_NAME));
        computeActual(format("ANALYZE \"%s/s1\".t", BUCKET_NAME));
        MaterializedResult showStatsResult = computeActual(format("SHOW STATS FOR \"%s/s1\".t", BUCKET_NAME));
        assertQuery(format("SHOW STATS FOR \"%s/s1\".t", BUCKET_NAME),
                "SELECT * FROM VALUES " +
                        "('c1',  null,    3.0,    0.0, null, 1, 3), " +
                        "('c2',  18.0,    2.0,    0.0, null, null, null), " +
                        "(null,  null,    null,   null,    3.0, null, null)");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }
}
