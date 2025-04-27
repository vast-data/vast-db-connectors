/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.VastClient;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.math.BigInt;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static java.lang.String.format;

public final class StatsUtils
{
    private StatsUtils() {}

    private static final Logger LOG = LoggerFactory.getLogger(StatsUtils.class);

    public static Attribute fieldToAttribute(StructField field, StructType schema)
    {
        int fieldIndex = (int) schema.getFieldIndex(field.name()).get();
        return new AttributeReference(field.name(),
                field.dataType(), field.nullable(), field.metadata(), ExprId.apply(fieldIndex),
                (Seq<String>) Seq$.MODULE$.<String>empty());
    }

    public static Statistics vastTableStatsToCatalystStatistics(VastStatistics tableStats)
    {
        return new Statistics(
                BigInt.apply(tableStats.getSizeInBytes()),
                Option.apply(BigInt.apply(tableStats.getNumRows())),
                AttributeMap$.MODULE$.empty(),
                false
        );
    }

    public static TableLevelStatistics sparkCatalystStatsToTableStatistics(Statistics fullStats)
    {
        final OptionalLong sizeInBytes = OptionalLong.of(fullStats.sizeInBytes().toLong());
        final OptionalLong rowCount = fullStats.rowCount().isDefined()
                ? OptionalLong.of(fullStats.rowCount().get().toLong())
                : OptionalLong.empty();
        final Map<NamedReference, ColumnStat> columnStats = new HashMap<>();
        fullStats.attributeStats().foreach(entry -> {
            NamedReference ref = FieldReference.apply(entry._1.name());
            return columnStats.put(ref, entry._2);
        });
        return new TableLevelStatistics(sizeInBytes, rowCount, columnStats);
    }

    public static VastStatistics getTableLevelStats(VastClient client, String schemaName, String tableName)
    {
        VastTransactionHandleManager<SimpleVastTransaction> transactionsManager = VastSparkTransactionsManager.getInstance(client, new VastSimpleTransactionFactory());
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(client,
                () -> transactionsManager.startTransaction(new StartTransactionContext(true, true)))) {
            // compute statistics via RPC
            VastStatistics tableStats = client.getTableStats(tx, schemaName, tableName);
            if (tableStats != null) {
                LOG.debug("Fetched statistics for table {}, statistics: numRows={}, sizeInBytes={}",
                        tableName, tableStats.getNumRows(), tableStats.getSizeInBytes());
                return tableStats;
            }
            else {
                throw new RuntimeException(format("Failed fetching table level stats: %s.%s", schemaName, tableName));
            }
        } catch (Exception e) {
            throw VastExceptionFactory.toRuntime(e);
        }
    }

    public static VastClient getVastClient()
    {
        try {
             return NDB.getVastClient(NDB.getConfig());
        }
        catch (VastUserException e) {
            throw new RuntimeException(e);
        }
    }
}
