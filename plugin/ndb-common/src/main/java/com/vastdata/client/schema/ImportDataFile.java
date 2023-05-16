/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ImportDataFile
{
    private final boolean hasVectorSchemaRoot;
    private final String srcBucket;
    private final String srcFile;
    private final Map<String, String> defaults;
    private Map<Field, String> fieldValuesMap;
    private final Optional<VectorSchemaRoot> vectorSchemaRoot;

    public ImportDataFile(String srcBucket, String srcFile, Map<String, String> defaults)
    {
        this.srcBucket = srcBucket;
        this.srcFile = srcFile;
        this.defaults = defaults;
        this.hasVectorSchemaRoot = false;
        this.vectorSchemaRoot = Optional.empty();
    }

    public ImportDataFile(String srcBucket, String srcFile, VectorSchemaRoot vectorSchemaRoot)
    {
        this.srcBucket = srcBucket;
        this.srcFile = srcFile;
        this.defaults = null;
        this.hasVectorSchemaRoot = true;
        this.vectorSchemaRoot = Optional.ofNullable(vectorSchemaRoot);
    }

    public boolean hasSchemaRoot()
    {
        return hasVectorSchemaRoot;
    }

    public Optional<VectorSchemaRoot> getVectorSchemaRoot()
    {
        return vectorSchemaRoot;
    }

    public String getSrcFile()
    {
        return this.srcFile;
    }

    public String getSrcBucket()
    {
        return this.srcBucket;
    }

    public Map<Field, String> getFieldValuesMap()
    {
        if (fieldValuesMap == null) { // For sanity, should not happen in Trino use case
            throw new IllegalStateException(format("Partitioning was not initialized on file %s", srcFile));
        }
        return fieldValuesMap;
    }

    public void setFieldsDefaultValues(Map<String, Field> tableFieldsMap)
    {
        Map<Field, String> fieldValuesMap = new HashMap<>();
        if (!defaults.isEmpty()) {
            defaults.forEach((name, val) -> {
                Field field = tableFieldsMap.get(name);
                checkArgument(field != null, format("Column %s doesn't exist in table", name));
                fieldValuesMap.put(field, val);
            });
        }
        this.fieldValuesMap = fieldValuesMap;
    }

    @Override
    public String toString()
    {
        return "ImportDataFile{" +
                "hasVectorSchemaRoot=" + hasVectorSchemaRoot +
                ", srcBucket='" + srcBucket + '\'' +
                ", srcFile='" + srcFile + '\'' +
                ", defaults=" + defaults +
                ", fieldValuesMap=" + fieldValuesMap +
                ", vectorSchemaRoot=" + vectorSchemaRoot +
                '}';
    }
}
