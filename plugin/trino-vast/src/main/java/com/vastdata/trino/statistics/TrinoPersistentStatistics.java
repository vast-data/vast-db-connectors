/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.stats.StatisticsUrl;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import com.vastdata.client.stats.VastStatisticsStorage;
import com.vastdata.trino.VastTableHandle;
import com.vastdata.trino.VastTrinoDependenciesFactory;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.statistics.TableStatistics;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;

public class TrinoPersistentStatistics
        implements VastStatisticsStorage<VastTableHandle, TableStatistics>
{
    private static final Logger LOG = Logger.get(TrinoPersistentStatistics.class);

    private final VastClient client;

    private final ObjectMapper mapper = TrinoStatisticsMapper.instance();

    private final LoadingCache<CacheKey, Optional<TableStatistics>> cache;
    private final StatisticsUrlExtractor<VastTableHandle> statisticsUrlHelper;
    private final String tag;
    private final Function<TableStatistics, String> statsSerializer;

    public TrinoPersistentStatistics(VastClient client, VastConfig config){
        this(client, config, null);
    }

    @VisibleForTesting
    protected TrinoPersistentStatistics(VastClient client, VastConfig config, Function<TableStatistics, String> statsSerializer)
    {
        statisticsUrlHelper = new VastTrinoDependenciesFactory().getStatisticsUrlHelper();
        tag = new VastTrinoDependenciesFactory().getConnectorVersionedStatisticsTag();
        this.client = client;
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMaxStatisticsFilesSupportedPerSession())
                .build(CacheLoader.from(this::tableStatisticsLoader));
        this.statsSerializer = Objects.requireNonNullElseGet(statsSerializer, () -> stats -> {
            try {
                return mapper.writeValueAsString(new TrinoSerializableTableStatistics(stats));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Optional<TableStatistics> tableStatisticsLoader(CacheKey tableHandleCacheKey)
    {
        StatisticsUrl extracted = StatisticsUrl.extract(tableHandleCacheKey.getTableHandle(), statisticsUrlHelper, tag);
        String bucketName = extracted.getBucket();
        String keyName = extracted.getKey();
        if (bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
            //Big Catalog tables do not persist, therefor don't need to undergo serialization
            return Optional.empty();
        }
        Optional<String> tsBuffer = client.s3GetObj(keyName, bucketName);
        LOG.info("fetched object %s from Vast: %s", keyName, tsBuffer);
        Optional<TableStatistics> result = tsBuffer.map(bufferBytes -> {
            try {
                TrinoSerializableTableStatistics serializableTableStatistics = mapper.readValue(bufferBytes, TrinoSerializableTableStatistics.class);
                TableStatistics ts = serializableTableStatistics.getTableStatistics();
                LOG.info("Parsed %s from S3 bucket %s", keyName, bucketName);
                return ts;
            } catch (IOException e) {
                LOG.warn(e, "Failed to parse table statistics: %s", bufferBytes);
                return null;
            }
        });
        LOG.debug("tableStatisticsLoader: result=%s " + result);
        return result;
    }

    @Override
    public Optional<TableStatistics> getTableStatistics(VastTableHandle table)
    {
        CacheKey cacheKey = new CacheKey(table);
        try {
            LOG.info("attempting to fetch statistics file for table %s from cache", table.toSchemaTableName());
            Optional<TableStatistics> result = cache.get(cacheKey);
            if (result.isEmpty()) {
                LOG.debug("statistics cache miss for table %s", table.toSchemaTableName());
            } else {
                LOG.debug("statistics cache hit for table %s", table.toSchemaTableName());
            }
            return result;
        } catch (Exception e) {
            LOG.info("failed to fetch statistics for table %s from cache\n", table);
            return Optional.empty();
        }
    }

    @Override
    public void setTableStatistics(VastTableHandle table, TableStatistics tableStatistics)
    {
        CacheKey cacheKey = new CacheKey(table);
        StatisticsUrl extracted = StatisticsUrl.extract(table, statisticsUrlHelper, tag);
        String bucketName = extracted.getBucket();
        String keyName = extracted.getKey();
        if (!bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
            // Big Catalog table statistics are not persistent and stored directly into the cache via cache.get
            LOG.info("uploading statistics file {} to S3 bucket {}...\n", keyName, bucketName);
            try {
                String tableStatisticsStr = statsSerializer.apply(tableStatistics);
                client.s3PutObj(keyName, bucketName, tableStatisticsStr);
                LOG.info("storing table statistics %s in cache\n", keyName);
            } catch (RuntimeException e) {
                LOG.warn("failed to upload table statistics file to S3 bucket %s", bucketName);
                throw e;
            }
        }
        this.cache.invalidate(cacheKey);
        try {
            this.cache.get(cacheKey, () -> Optional.of(tableStatistics));
        } catch (ExecutionException e) {
            LOG.info("cache(Table Statistics) load failed: %s", e);
        }
        LOG.info("uploaded table statistics file to S3 bucket %s", bucketName);
    }

    @Override
    public void deleteTableStatistics(VastTableHandle table)
    {
        CacheKey tableHandleCacheKey = new CacheKey(table);
        LOG.info("invalidating statistics for table %s from cache", tableHandleCacheKey.getTableHandle());
        // Deletion of statistics files occur in cpp code upon deletion of a table
        cache.invalidate(tableHandleCacheKey);
    }

// This class purpose is to be used as a key for table statistics cache.
// Originally VastTableHandle was used as key, however, VastTableHandle equals method consider all the classes members
// including predicates for example, which caused a scenario where same table with different predicates could lead to cache miss
// and redundant s3 calls to fetch the statists files from Vast (ORION-148783)
    public static class CacheKey
    {
        private VastTableHandle tableHandle;

        public CacheKey(VastTableHandle tableHandle)
        {
            this.tableHandle = tableHandle;
        }

        public VastTableHandle getTableHandle()
        {
            return tableHandle;
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
            return Objects.equals(this.tableHandle.getHandleID(), other.tableHandle.getHandleID());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle.getHandleID());
        }
    }
}
