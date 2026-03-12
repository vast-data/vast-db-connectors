/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.ParsedURL;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.stats.StatisticsUrl;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.stats.VastStatisticsStorage;
import com.vastdata.spark.VastTable;
import ndb.NDB;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Table;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.cache.CacheBuilder;
import org.sparkproject.guava.cache.CacheLoader;
import org.sparkproject.guava.cache.LoadingCache;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;
import static com.vastdata.spark.statistics.StatsUtils.vastTableStatsToCatalystStatistics;
import static java.lang.String.format;

public final class SparkPersistentStatistics implements VastStatisticsStorage<VastTable, Statistics>
{
    private static final Logger LOG = LoggerFactory.getLogger(SparkPersistentStatistics.class);

    private final VastClient client;
    private final StatisticsUrlExtractor<Table> statisticsUrlHelper = (StatisticsUrlExtractor<Table>) NDB.getSparkDependenciesFactory().getStatisticsUrlHelper();
    private final String tag = NDB.getSparkDependenciesFactory().getConnectorVersionedStatisticsTag();

    private ObjectMapper getMapper(VastTable table)
    {
        return SparkStatisticsMapper.instance(table);
    }

    private final LoadingCache<CacheKey, Optional<Statistics>> cache;

    public SparkPersistentStatistics(VastClient client, VastConfig config)
    {
        this.client = client;
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(config.getMaxStatisticsFilesSupportedPerSession())
            .build(getCacheLoader(client));
    }

    @NotNull
    private CacheLoader<CacheKey, Optional<Statistics>> getCacheLoader(VastClient client)
    {
        return new CacheLoader<CacheKey, Optional<Statistics>>()
        {
            @Override
            public Optional<Statistics> load(@NotNull CacheKey tableCacheKey)
            {
                VastTable table = tableCacheKey.getTable();
                StatisticsUrl statisticsUrl = StatisticsUrl.extract(table, statisticsUrlHelper, tag);
                String bucketName = statisticsUrl.getBucket();
                String keyName = statisticsUrl.getKey();
                if (bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
                    //Big Catalog bucket does allow persisting files via S3 protocol
                    return Optional.empty();
                }
                Optional<String> tsBuffer = client.s3GetObj(keyName, bucketName);
                LOG.debug("Fetched object {} from Vast", keyName);
                if ((tsBuffer != null) && (tsBuffer.isPresent())) {
                    try {
                        Statistics ts = getMapper(table).readValue(tsBuffer.get(), Statistics.class);
                        LOG.debug("Fetched {} statistics file from S3 bucket {}: {}", keyName, bucketName, ts);
                        return Optional.of(ts);
                    }
                    catch (Exception e) {
                        LOG.error(format("Failed to parse table statistics file, perhaps table wasn't analyzed or corrupted statistics file: %s", tsBuffer.get()), e);
                        return Optional.empty();
                    }
                } else
                // In case the table weren't analyzed we would like to execute an RPC for only the table level stats.
                // https://vastdata.atlassian.net/browse/ORION-122503
                // We do not want it to persist since no explicit analyze was called by the user
                {
                    //Fetch table level statistics
                    try {
                        ParsedURL parsedURL = ParsedURL.of(table.name());
                        String schemaPath = parsedURL.getFullSchemaPath();
                        // compute statistics via RPC
                        VastStatistics tableStats = StatsUtils.getTableLevelStats(client, schemaPath, parsedURL.getTableName());
                        //populate the cache
                        org.apache.spark.sql.catalyst.plans.logical.Statistics newStats = vastTableStatsToCatalystStatistics(tableStats);
                        LOG.debug("Converted statistics for table {} to spark persistent statistics: {}", table.name(), newStats.simpleString());
                        return Optional.of(newStats);
                    } catch (Exception e) {
                        throw VastExceptionFactory.toRuntime("Failed fetching table level statistics", e);
                    }
                }
            }
        };
    }

    @Override
    public Optional<Statistics> getTableStatistics(VastTable table) {
        CacheKey tableCacheKey = new CacheKey(table);
        try {
            LOG.debug("Fetching statistics file for table {} from cache", table.name());
            Optional<Statistics> result = cache.get(tableCacheKey);
            if (result.isPresent()) {
                LOG.debug("Successfully fetched statistics file for table {}", table.name());
            }
            else {
                LOG.debug("Could not fetch statistics file for table {}", table.name());
            }
            return result;
        } catch (ExecutionException | RuntimeException e) {
            LOG.warn(format("Failed to fetch statistics file for table %s from cache", table.name()), e);
            return Optional.empty();
        }
    }

    @Override
    public void setTableStatistics(VastTable table, Statistics tableStatistics) {
        CacheKey tableCacheKey = new CacheKey(table);
        StatisticsUrl statisticsUrl = StatisticsUrl.extract(table, statisticsUrlHelper, tag);
        String bucketName = statisticsUrl.getBucket();
        String keyName = statisticsUrl.getKey();
        if (!bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
            // Big Catalog table statistics are not persistent and stored directly into the cache via cache.get
            LOG.debug("Uploading statistics file {} to S3 bucket {}", keyName, bucketName);
            try {
                String tableStatisticsStr = getMapper(table).writeValueAsString(tableStatistics);
                LOG.debug("Storing table statistics: file name: {}, contents: {}, bucket: {}", keyName, tableStatisticsStr, bucketName);
                client.s3PutObj(keyName, bucketName, tableStatisticsStr);
            } catch (Exception e) {
                LOG.warn("Failed to upload table statistics file to S3 bucket {}", bucketName);
                throw new RuntimeException(e);
            }
        }
        cache.invalidate(tableCacheKey);
        try {
            cache.get(tableCacheKey, () -> Optional.of(tableStatistics));
        } catch (ExecutionException e) {
            LOG.warn("Cache(Table Statistics) load failed", e);
        }
        LOG.debug("Uploaded table statistics file to S3 bucket {}", bucketName);
    }

    @Override
    public void deleteTableStatistics(VastTable table) {
        CacheKey tableCacheKey = new CacheKey(table);
        LOG.debug("Invalidating statistics cache for table {}", table.name());
        // Deletion of statistics files occur in cpp code upon deletion of a table
        cache.invalidate(tableCacheKey);
    }

// This class purpose is to be used as a key for table statistics cache.
// Originally VastTable was used as key, however, VastTable equals method consider all the classes members
// including table name for example, which caused a scenario where same table with different predicates could lead to cache miss
// and redundant s3 calls to fetch the statists files from Vast (ORION-149173)
    public static class CacheKey
    {
        private final VastTable vastTable;

        public CacheKey(VastTable vastTable)
        {
            this.vastTable = vastTable;
        }

        public VastTable getTable()
        {
            return this.vastTable;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.getTable().getTableHandleID(), other.getTable().getTableHandleID());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.getTable().getTableHandleID());
        }
    }
}

