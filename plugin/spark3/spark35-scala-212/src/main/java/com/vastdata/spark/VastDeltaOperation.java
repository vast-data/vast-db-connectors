/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDelta;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_ROW_ID_NONNULL;

public abstract class VastDeltaOperation
        implements SupportsDelta
{
    private static final Logger LOG = LoggerFactory.getLogger(VastDeltaOperation.class);
    private static final NamedReference[] ROW_ID_REF;
    static {
        Builder<String, List<String>> objectSeqBuilder = List$.MODULE$.newBuilder();
        objectSeqBuilder.$plus$eq(VASTDB_SPARK_ROW_ID_NONNULL.getName());
        List<String> seq = objectSeqBuilder.result();
        ROW_ID_REF = new FieldReference[]{new FieldReference(seq)};
    }

    private final VastTable vastTable;
    public VastDeltaOperation(VastTable vastTable)
    {
        this.vastTable = vastTable;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap)
    {
        LOG.debug("newScanBuilder: {}", caseInsensitiveStringMap);
        return vastTable.newScanBuilder(caseInsensitiveStringMap);
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo)
    {
        LOG.debug("newWriteBuilder: {}", logicalWriteInfo);
        return vastTable.newWriteBuilder(logicalWriteInfo);
    }

    @Override
    public NamedReference[] rowId()
    {
        return ROW_ID_REF;
    }
}
