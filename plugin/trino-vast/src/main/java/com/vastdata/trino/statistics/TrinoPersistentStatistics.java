/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastStatisticsStorage;
import io.airlift.log.Logger;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.statistics.TableStatistics;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_BUCKET_NAME;

public class TrinoPersistentStatistics
        implements VastStatisticsStorage<String, TableStatistics>
{
    private static final Logger LOG = Logger.get(VastStatisticsManager.class);

    private final TrinoTableStatisticsIdentifierFactory factory = new TrinoTableStatisticsIdentifierFactory();

    private final VastClient client;

    private final ObjectMapper mapper = TrinoStatisticsMapper.instance();

    private final LoadingCache<String, Optional<TableStatistics>> cache;

    public TrinoPersistentStatistics(VastClient client, VastConfig config){
        this.client = client;
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMaxStatisticsFilesSupportedPerSession())
                .build(CacheLoader.from(this::tableStatisticsLoader));
    }

    private Optional<TableStatistics> tableStatisticsLoader(String tableUrl) {
        String bucketName = factory.getBucket(tableUrl);
        String keyName = factory.getKeyName(tableUrl);
        if (bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
            //Big Catalog tables do not persist, therefor don't need to undergo serialization
            return Optional.empty();
        }
        Optional<String> tsBuffer = client.s3GetObj(keyName, bucketName);
        LOG.info("Fetched object %s from Vast", keyName);
        if (tsBuffer.isPresent()) {
            try {
                TrinoSerializableTableStatistics serializableTableStatistics = mapper.readValue(tsBuffer.get(), TrinoSerializableTableStatistics.class);
                TableStatistics ts = serializableTableStatistics.getTableStatistics();
                LOG.info("Parsed %s from S3 bucket %s", keyName, bucketName);
                return Optional.of(ts);
            } catch (IOException e) {
                LOG.warn(e, "Failed to parse table statistics: %s", tsBuffer.get());
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableStatistics> getTableStatistics(String tableUrl) {
        try {
            LOG.info("Fetching table statistics file %s from cache\n", tableUrl);
            Optional<TableStatistics> result = cache.get(tableUrl);
            LOG.info("Successfully fetched table statistics file %s from cache\n", tableUrl);
            return result;
        } catch (Exception e) {
            LOG.info("Failed to fetch table statistics %s from cache\n", tableUrl);
            return Optional.empty();
        }
    }

    @Override
    public void setTableStatistics(String tableUrl, TableStatistics tableStatistics)
    {
        String keyName = factory.getKeyName(tableUrl);
        String bucketName = factory.getBucket(tableUrl);
        if (!bucketName.equals(BIG_CATALOG_BUCKET_NAME)) {
            // Big Catalog table statistics are not persistent and stored directly into the cache via cache.get
            LOG.info("Uploading statistics file {} to S3 bucket {}...\n", keyName, bucketName);
            try {
                String tableStatisticsStr = mapper.writeValueAsString(new TrinoSerializableTableStatistics(tableStatistics));
                client.s3PutObj(keyName, bucketName, tableStatisticsStr);
                LOG.info("Storing table statistics %s from cache\n", keyName);
            } catch (Exception e) {
                LOG.warn("Failed to upload table statistics file to S3 bucket %s", bucketName);
                throw new RuntimeException(e);
            }
        }
        this.cache.invalidate(tableUrl);
        try {
            this.cache.get(tableUrl, () -> Optional.of(tableStatistics));
        } catch (ExecutionException e) {
            LOG.info("Cache(Table Statistics) load failed: %s", e);
        }
        LOG.info("Uploaded table statistics file to S3 bucket %s", bucketName);
    }

    private class tableStatisticsCallable implements Callable<Optional<TableStatistics>> {
        private TableStatistics ts;
        public tableStatisticsCallable(TableStatistics ts){
            this.ts = ts;
        }
        @Override
        public Optional<TableStatistics> call() throws Exception {
            return Optional.of(this.ts);
        }
    }

    @Override
    public void deleteTableStatistics(String tableUrl) {
        LOG.info("Invalidating table statistics %s from cache\n", tableUrl);
        // Deletion of statistics files occur in cpp code upon deletion of a table
        cache.invalidate(tableUrl);
    }

}
