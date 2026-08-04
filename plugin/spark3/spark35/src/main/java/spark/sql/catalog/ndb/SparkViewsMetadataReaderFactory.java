/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.client.VastConfig;

public class SparkViewsMetadataReaderFactory
{
    private final VastConfig config;

    public SparkViewsMetadataReaderFactory(VastConfig config)
    {
        this.config = config;
    }

    SparkViewsMetadataReader instance()
    {
        return new SparkViewsMetadataReader(config);
    }
}
