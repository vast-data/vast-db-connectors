/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.partition;

import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.trino.VastConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestPartitionColumnMetadata
{
    @Test
    public void testValueOf_withTransformAndArg()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec(
                "bucket(col1, 16)");
        assertEquals("bucket", meta.getTransform());
        assertEquals("col1_bucket", meta.getColumnName());
        assertEquals("col1", meta.getSourceColumnName());
        assertEquals("16", meta.getArg());
    }

    @Test
    public void testValueOf_withTransformNoArg()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec(
                "year(col2)");
        assertEquals("year", meta.getTransform());
        assertEquals("col2_year", meta.getColumnName());
        assertEquals("col2", meta.getSourceColumnName());
        assertNull(meta.getArg());
    }

    @Test
    public void testValueOf_identityTransform()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec("col3");
        assertEquals("identity", meta.getTransform());
        assertEquals("col3", meta.getColumnName());
        assertNull(meta.getArg());
    }

    @Test
    public void testValueOf_emptyString()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec("");
        assertEquals("identity", meta.getTransform());
        assertEquals("", meta.getColumnName());
        assertNull(meta.getArg());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void testValueOf_nullString()
    {
        Assertions.assertThrows(NullPointerException.class,
                () -> VastConnector.parsePartitionSpec(null));
    }

    @Test
    public void testValueOf_malformedTransform()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec(
                "bucketcol1,16)");
        assertEquals("identity", meta.getTransform());
        assertEquals("bucketcol1,16)", meta.getColumnName());
        assertNull(meta.getArg());
    }

    @Test
    public void testValueOf_extraSpaces()
    {
        PartitionColumnMetadata meta = VastConnector.parsePartitionSpec(
                "  year(  col2  )  ");
        assertEquals("year", meta.getTransform());
        assertEquals("col2_year", meta.getColumnName());
        assertEquals("col2", meta.getSourceColumnName());
        assertNull(meta.getArg());
    }
}
