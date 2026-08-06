/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestVastSmokeIT
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "build";
    private static final String PROBE_TABLE_NAME = "probe";
    private static final String BUILD_TABLE_NAME = "build";
    //    private ObjectMapper objectMapper;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String keyId = fetchAwsCredential("AWS_ACCESS_KEY_ID");
        String accessKey = fetchAwsCredential("AWS_SECRET_ACCESS_KEY");
        return VastQueryRunner.createQueryRunner(
                Map.of("node.environment", "vast", "endpoint",
                        System.getProperty("ENDPOINT", "http://localhost:9090"),
                        "access_key_id", keyId, "secret_access_key", accessKey),
                "vast", 3, Map.of());
    }

    private String fetchAwsCredential(String name)
    {
        String cred = System.getenv(name);

        if (cred != null) {
            return cred;
        }

        // This matches the bash script that runs the server on dev vm via ssh
        Path credFilePath = Path.of("/tmp", name);

        if (!Files.exists(credFilePath)) {
            throw new RuntimeException("Credential " + name + " not found in environment variable or in file " + credFilePath);
        }

        try {
            return Files.readString(credFilePath);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read credential " + name, e);
        }
    }

    @BeforeAll
    public void init()
            throws Exception
    {
        assumeTrue(System.getenv("INTEG_TEST") != null,
                "Skipping all tests: INTEG_TEST not set");
        super.init();
    }

    @BeforeEach
    public void setUp()
    {
        //        computeActual(format("CREATE SCHEMA IF NOT EXISTS \"%s/s1\"", BUCKET_NAME));
        //        computeActual(format("CREATE TABLE IF NOT EXISTS \"%s/s1\".t (c1 INTEGER, c2 VARCHAR)", BUCKET_NAME));
    }

    @AfterEach
    public void tearDown()
    {
        //        computeActual(format("DROP TABLE \"%s/s1\".t", BUCKET_NAME));
        //        computeActual(format("DROP SCHEMA \"%s/s1\"", BUCKET_NAME));
    }

    @Test
    public void testSimple()
    {
        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (1, 'shlomi')",
                BUCKET_NAME));
        MaterializedResult materializedRows = computeActual(
                format("SELECT c1 FROM \"%s/s1\".t WHERE c1 = 1", BUCKET_NAME));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testAnalyze()
    {
        Session session = TestingSession
                .testSessionBuilder()
                .setCatalog("vast")
                .setCatalogSessionProperty("vast",
                        VastSessionProperties.USE_TICKET_GLOBAL_ENDPOINT,
                        "true")
                .build();
        //        MaterializedResult materializedRows = computeActual(session, "select records from TABLE(vast.execute('select id from vastdb.\"test-adbc-vector-search-bucket/fslschema\".fsltable'))");
        //        assertThat(materializedRows.getRowCount()).isEqualTo(1000);
        //        materializedRows = computeActual(session, "select records from TABLE(vast.execute('DESCRIBE vastdb.\"test-adbc-vector-search-bucket/fslschema\".fsltable'))");
        MaterializedResult materializedRows = computeActual(session,
                "select records from TABLE(vast.execute('SELECT * FROM vastdb.\"qe-simple/s\".t', false))");
        //        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (1, 'shlomi')", BUCKET_NAME));
        //        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (2, 'alfasi')", BUCKET_NAME));
        //        computeActual(format("INSERT INTO \"%s/s1\".t VALUES (3, 'shlomi')", BUCKET_NAME));
        //        MaterializedResult materializedRows = computeActual(format("SELECT c1 FROM \"%s/s1\".t WHERE c1 = 1", BUCKET_NAME));
        //        computeActual(format("ANALYZE \"%s/s1\".t WITH (columns = ARRAY['c1'])", BUCKET_NAME));
        //        computeActual(format("ANALYZE \"%s/s1\".t", BUCKET_NAME));
        //        MaterializedResult showStatsResult = computeActual(format("SHOW STATS FOR \"%s/s1\".t", BUCKET_NAME));
        //        assertQuery(format("SHOW STATS FOR \"%s/s1\".t", BUCKET_NAME),
        //                "SELECT * FROM VALUES " +
        //                        "('c1',  null,    3.0,    0.0, null, 1, 3), " +
        //                        "('c2',  18.0,    2.0,    0.0, null, null, null), " +
        //                        "(null,  null,    null,   null,    3.0, null, null)");
        //        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testJoin()
            throws JsonProcessingException
    {
        computeActual(
                format("CREATE TABLE IF NOT EXISTS \"%s/s1\".%s (build_c1 INTEGER, build_c2 VARCHAR) WITH (sorted_by = ARRAY['build_c2'])",
                        BUCKET_NAME, BUILD_TABLE_NAME));
        computeActual(
                format("CREATE TABLE IF NOT EXISTS \"%s/s1\".%s (probe_c1 INTEGER, probe_c2 VARCHAR) WITH (sorted_by = ARRAY['probe_c2'])",
                        BUCKET_NAME, PROBE_TABLE_NAME));

        QueryRunner.MaterializedResultWithPlan selectPlan = getQueryRunner().executeWithPlan(
                getSession(), format("SELECT * FROM \"%s/s1\".%s", BUCKET_NAME,
                        BUILD_TABLE_NAME));
        printJson(selectPlan);

        UUID joinUUID = UUID.randomUUID();
        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (1, '%s')", BUCKET_NAME,
                        BUILD_TABLE_NAME,
                        UUID.randomUUID().toString().toUpperCase(Locale.ROOT)));
        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (2, '%s')", BUCKET_NAME,
                        BUILD_TABLE_NAME,
                        joinUUID.toString().toUpperCase(Locale.ROOT)));
        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (3, '%s')", BUCKET_NAME,
                        BUILD_TABLE_NAME,
                        UUID.randomUUID().toString().toUpperCase(Locale.ROOT)));

        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (1, '%s')", BUCKET_NAME,
                        PROBE_TABLE_NAME,
                        UUID.randomUUID().toString().toUpperCase(Locale.ROOT)));
        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (2, '%s')", BUCKET_NAME,
                        PROBE_TABLE_NAME,
                        joinUUID.toString().toUpperCase(Locale.ROOT)));
        computeActual(
                format("INSERT INTO \"%s/s1\".%s VALUES (3, '%s')", BUCKET_NAME,
                        PROBE_TABLE_NAME,
                        UUID.randomUUID().toString().toUpperCase(Locale.ROOT)));

        selectPlan = getQueryRunner().executeWithPlan(getSession(),
                format("SELECT build_c1 FROM \"%s/s1\".%s WHERE build_c2 > '00' AND build_c2 < 'FF'",
                        BUCKET_NAME, BUILD_TABLE_NAME));
        printJson(selectPlan);

        selectPlan = getQueryRunner().executeWithPlan(getSession(),
                format("SELECT b.build_c1, p.probe_c2 FROM \"%s/s1\".%s b JOIN \"%s/s1\".%s p ON b.build_c2 = p.probe_c2",
                        BUCKET_NAME, BUILD_TABLE_NAME, BUCKET_NAME,
                        PROBE_TABLE_NAME));
        printJson(selectPlan);

        computeActual(format("DROP TABLE IF EXISTS \"%s/s1\".%s", BUCKET_NAME,
                BUILD_TABLE_NAME));
        computeActual(format("DROP TABLE IF EXISTS \"%s/s1\".%s", BUCKET_NAME,
                PROBE_TABLE_NAME));
    }

    private void printJson(QueryRunner.MaterializedResultWithPlan selectPlan)
    {
        QueryInfo fullQueryInfo = getQueryRunner()
                .getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(selectPlan.queryId());
        System.out.println(fullQueryInfo);
    }

    @Test
    public void testColocatedJoin()
    {
        QueryRunner.MaterializedResultWithPlan materializedRows = getQueryRunner().executeWithPlan(
                getSession(),
                "SELECT count(*) FROM vast.\"join-bucket/join-schema\".probe JOIN vast.\"join-bucket/join-schema\".build ON probe_uuid = build_uuid");
        QueryInfo qi = getQueryRunner()
                .getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(materializedRows.queryId());
        List<OperatorStats> operatorSummaries = qi
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(operatorStats -> operatorStats
                        .getOperatorType()
                        .contains("Scan"))
                .toList();
        operatorSummaries.forEach(operatorStats -> assertThat(
                operatorStats.getOutputPositions()).isEqualTo(100));
    }

    @Test
    public void testNonColocatedJoin()
    {
        QueryRunner.MaterializedResultWithPlan materializedRows = getQueryRunner().executeWithPlan(
                getSession(),
                "SELECT count(*) FROM vast.\"join-bucket/join-schema\".probe JOIN vast.\"join-bucket/join-schema\".build ON probe_int = build_int");
        QueryInfo qi = getQueryRunner()
                .getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(materializedRows.queryId());
        List<OperatorStats> operatorSummaries = qi
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(operatorStats -> operatorStats
                        .getOperatorType()
                        .contains("Scan"))
                .toList();
        operatorSummaries.forEach(operatorStats -> assertThat(
                operatorStats.getOutputPositions()).isEqualTo(100));
    }

    @Test
    public void testColocatedJoinWithFilter()
    {
        QueryRunner.MaterializedResultWithPlan materializedRows = getQueryRunner().executeWithPlan(
                getSession(),
                "SELECT count(*) FROM vast.\"join-bucket/join-schema\".probe JOIN vast.\"join-bucket/join-schema\".build ON probe_uuid = build_uuid WHERE probe_int = 1");
        QueryInfo qi = getQueryRunner()
                .getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(materializedRows.queryId());
        List<OperatorStats> operatorSummaries = qi
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(operatorStats -> operatorStats
                        .getOperatorType()
                        .contains("Scan"))
                .toList();
        operatorSummaries.forEach(operatorStats -> assertThat(
                operatorStats.getOutputPositions()).isEqualTo(100));
    }

    @Test
    public void testPlayground()
    {
        var queryRunner = this.getQueryRunner();
        var session = this.getSession();
        var schemaName = "vast.\"vastdb/s\"";
        var tableName = "vast.\"vastdb/s\".\"t\"";

        queryRunner.executeWithPlan(session, String.format("DROP TABLE IF EXISTS %s", tableName));
        queryRunner.executeWithPlan(session, String.format("DROP SCHEMA IF EXISTS %s", schemaName));

        queryRunner.executeWithPlan(session, String.format("CREATE SCHEMA %s", schemaName));
        queryRunner.executeWithPlan(session, String.format("CREATE TABLE %s (a INTEGER, b INTEGER, c INTEGER) " +
                "WITH (partitioning = ARRAY['a', 'bucket(b, 4)'])", tableName));

        queryRunner.executeWithPlan(session, String.format("INSERT INTO %s VALUES " +
                "(1, 2, 3), " +
                "(4, 5, 6), " +
                "(7, 8, 9)", tableName));

        queryRunner.executeWithPlan(session, String.format(
                    "INSERT INTO %s VALUES ('l', 9, 4, 3, 'e'), ('m', 9, 3, 2, 'd'), ('n', 9, 1, NULL, 'c'), ('o', 9, 1, 1, 'd'), ('p', 9, 2, 2, 'd')",
                    tableName));

        var result = queryRunner.executeWithPlan(session, String.format("SELECT * FROM %s WHERE b > 6", tableName));
        System.out.println(result);
    }
}
