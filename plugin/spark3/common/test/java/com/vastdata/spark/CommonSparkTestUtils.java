/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.spark;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class CommonSparkTestUtils
{
    public static final Map<String, Object> DEFAULT_SPARK_CONF = new HashMap<>();

    private static String fetchAwsKey(String environmentVariableName)
    {
        String environmentVariable = System.getenv(environmentVariableName);

        if (environmentVariable != null) {
            return environmentVariable;
        }

        // This matches the bash script that runs the server on dev vm via ssh
        Path keyFilePath = Path.of("/tmp", environmentVariableName);

        if (!Files.exists(keyFilePath)) {
            return "NO_KEY_SUPPLIED";
        }

        try {
            return Files.readString(keyFilePath);
        }
        catch (IOException e) {
            return "FILE_READING_FAILED";
        }
    }

    static {
        DEFAULT_SPARK_CONF.put("spark.sql.catalog.ndb",
                "spark.sql.catalog.ndb.VastCatalog");
        DEFAULT_SPARK_CONF.put("spark.sql.defaultCatalog", "ndb");
        DEFAULT_SPARK_CONF.put("spark.sql.extensions",
                "ndb.NDBSparkSessionExtension");
        DEFAULT_SPARK_CONF.put("spark.sql.readSideCharPadding", false);
        DEFAULT_SPARK_CONF.put(
                "spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled",
                "true");
        DEFAULT_SPARK_CONF.put("spark.driver.extraClassPath",
                "src/tabular/trino/plugin/spark3/resources/*");
        DEFAULT_SPARK_CONF.put("spark.driver.userClassPathFirst", true);
        DEFAULT_SPARK_CONF.put("spark.driver.bindAddress", "127.0.0.1");
        DEFAULT_SPARK_CONF.put("spark.driver.host", "127.0.0.1");
        DEFAULT_SPARK_CONF.put("spark.ndb.access_key_id",
                fetchAwsKey("AWS_ACCESS_KEY_ID"));
        DEFAULT_SPARK_CONF.put("spark.ndb.secret_access_key",
                fetchAwsKey("AWS_SECRET_ACCESS_KEY"));
        DEFAULT_SPARK_CONF.put("spark.ndb.num_of_splits", "2");
        DEFAULT_SPARK_CONF.put(
                "spark.ndb.vast_transaction_keep_alive_interval_seconds", 1);
    }

    private CommonSparkTestUtils()
    {
    }

    public static class TestListener
            implements ITestListener
    {
        @Override
        public void onTestStart(ITestResult result)
        {
            System.out.println("Starting test: " + result
                    .getTestClass()
                    .getName() + "::" + result.getName());
        }

        @Override
        public void onTestSuccess(ITestResult result)
        {
            try {
                releaseSession();
            }
            finally {
                System.out.println("Test passed: " + result
                        .getTestClass()
                        .getName() + "::" + result.getName() + " in " + (result.getEndMillis() - result.getStartMillis()) + " ms");
            }
        }

        @Override
        public void onTestFailure(ITestResult result)
        {
            try {
                releaseSession();
            }
            finally {
                System.out.println("Test failed: " + result
                        .getTestClass()
                        .getName() + "::" + result.getName() + " in " + (result.getEndMillis() - result.getStartMillis()) + " ms");
            }
        }

        private void releaseSession()
        {
            try {
                Class<?> sparkSessionClass = Class.forName(
                        "org.apache.spark.sql.SparkSession");

                Method clearActiveSession = sparkSessionClass.getMethod(
                        "clearActiveSession");
                Method clearDefaultSession = sparkSessionClass.getMethod(
                        "clearDefaultSession");
                Method closeSession = sparkSessionClass.getMethod("close");
                Method getActiveSession = sparkSessionClass.getMethod(
                        "getActiveSession");
                Object activeSessionOption = getActiveSession.invoke(null);
                Method activeSessionIsEmpty = activeSessionOption
                        .getClass()
                        .getMethod("isEmpty");
                if (!(boolean) activeSessionIsEmpty.invoke(
                        activeSessionOption)) {
                    Method activeSessionGet = activeSessionOption
                            .getClass()
                            .getMethod("get");
                    closeSession.invoke(
                            activeSessionGet.invoke(activeSessionOption));
                    clearActiveSession.invoke(null);
                    clearDefaultSession.invoke(null);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onTestSkipped(ITestResult result)
        {
        }

        @Override
        public void onTestFailedButWithinSuccessPercentage(ITestResult result)
        {
        }

        @Override
        public void onStart(ITestContext context)
        {
            System.out.println("Test start: " + context.getClass());
        }

        @Override
        public void onFinish(ITestContext context)
        {
            System.out.println("Test finished: " + context.getClass());
        }
    }
}
