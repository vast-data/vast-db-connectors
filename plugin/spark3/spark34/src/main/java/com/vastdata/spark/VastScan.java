/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.metrics.EmptyPagesCount;
import com.vastdata.spark.metrics.EmptyPartitionsCount;
import com.vastdata.spark.metrics.PageSizeAVG;
import com.vastdata.spark.metrics.SplitFetchIdleTimeMetric;
import com.vastdata.spark.metrics.SplitFetchTimeMetric;
import com.vastdata.spark.metrics.SplitGetIdleTime;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import com.vastdata.spark.statistics.FilterEstimator;
import com.vastdata.spark.statistics.TableLevelStatistics;
import ndb.NDB;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.AlwaysFalse;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.vastdata.OptionalPrimitiveHelpers.map;
import static com.vastdata.client.VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD;
import static com.vastdata.client.VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.spark.AlwaysFalseFilterUtil.getAlwaysFalsePredicates;
import static com.vastdata.spark.predicate.VastPredicatePushdown.MIN_MAX_FULL_RANGE_ONLY;
import static java.lang.String.format;

public class VastScan
        implements Scan, SupportsReportStatistics, SupportsRuntimeV2Filtering
{
    private static final Logger LOG = LoggerFactory.getLogger(VastScan.class);
    private static final SecureRandom scanIDProvider = new SecureRandom();
    private final VastTable table;
    private final StructType schema;
    private final Integer limit;
    private final int runtimeFiltersCompactThreshold;
    private final int minMaxCompactionMinValuesThreshold;
    private final int filterValueSizeLimit;
    @VisibleForTesting
    protected List<List<VastPredicate>> pushDownPredicates;
    private boolean pushDownPredicatesChanged = true;

    private VastBatch vastBatch;
    private final int scanID = scanIDProvider.nextInt();
    private Statistics scanStatistics;
    private static final AtomicBoolean describeFlag = new AtomicBoolean(false);
    private final boolean verbose = describeFlag.getAndSet(false);

    public VastScan(int scanBuilderID, VastTable table, StructType schema, Integer limit, List<List<VastPredicate>> predicates)
            throws VastUserException
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.pushDownPredicates = predicates;
        try {
            this.runtimeFiltersCompactThreshold = NDB.getConfig().getDynamicFilterCompactionThreshold();
            this.minMaxCompactionMinValuesThreshold = NDB.getConfig().getMinMaxCompactionMinValuesThreshold();
            int runtimeFilterMaxValuesThreshold = NDB.getConfig().getDynamicFilterMaxValuesThreshold();
            this.filterValueSizeLimit = Math.max(runtimeFilterMaxValuesThreshold, this.runtimeFiltersCompactThreshold);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }

        LOG.warn("new VastScan: table={}, scanBuilderID={}, scanID={}", table.name(), scanBuilderID, this.scanID);
    }

    @Override
    public StructType readSchema()
    {
        return schema;
    }


    private String formatSchemedName()
    {
        return
            "(" +
            this.table.name() +
            ", " +
            this.scanID +
            ")";
    }

    private List<List<String>> formatPredicates()
    {
        return this.pushDownPredicates
                .stream()
                .map(vpList ->
                        vpList
                                .stream()
                                .map(VastPredicate::toString)
                                .collect(Collectors.toList())
                )
                .collect(Collectors.toList());
    }

    @Override
    public String description() {
        return toStringHelper(this)
                .add("schemed_name", this.formatSchemedName())
                .add("pushed_down_limit", this.limit)
                .add("pushed_down_predicates", this.formatPredicates())
                .toString();
    }

    @Override
    @JsonIgnore
    public VastBatch toBatch()
    {
        LOG.warn("{} toBatch", this);
        if (vastBatch == null) {
            vastBatch = new VastBatch(table, schema, limit, pushDownPredicates);
        }
        else {
            if (pushDownPredicatesChanged) {
                vastBatch.updatePushdownPredicates(pushDownPredicates);
            }
        }
        return vastBatch;
    }

    @Override
    public Statistics estimateStatistics() {
        if (verbose) {
            LOG.warn(format("%s estimateStatistics", this), new Exception("VERBOSE SCAN"));
        }
        else {
            LOG.warn("{} estimateStatistics", this);
        }
        if (scanStatistics == null) {
            final TableLevelStatistics scanStatistics = estimateBasedOnTableStatistics((TableLevelStatistics) this.table.estimateStatistics());
            this.scanStatistics = FilterEstimator.estimateStatistics(this.pushDownPredicates, scanStatistics);
            LOG.info("Estimated statistics: row count: {} -> {}, size in bytes: {} -> {}",
                    scanStatistics.numRows(), this.scanStatistics.numRows(), scanStatistics.sizeInBytes(), this.scanStatistics.sizeInBytes());
        }
        return scanStatistics;
    }

    @NotNull
    private TableLevelStatistics estimateBasedOnTableStatistics(TableLevelStatistics statistics)
    {
        final OptionalLong sizeStatistics = statistics.sizeInBytes();
        if (sizeStatistics.isPresent()) {
            final int tableSchemaDefaultSize = this.table.schema().defaultSize();
            final float scanSchemaSize = (float) this.readSchema().defaultSize();
            final long currentSize = sizeStatistics.getAsLong();
            final long newSize = getNewSize(tableSchemaDefaultSize, scanSchemaSize, currentSize);
            return new TableLevelStatistics(OptionalLong.of(newSize), statistics.numRows(), statistics.columnStats());
        }
        else {
            LOG.warn("{} Table size is not available", this);
            return statistics;
        }
    }

    protected long getNewSize(int tableSchemaDefaultSize, float scanSchemaSize, long currentSize)
    {
        final float ratio = scanSchemaSize / tableSchemaDefaultSize;
        final long newSize = (long) (currentSize * ratio);
        LOG.warn("{} Decreasing scan estimation size from {} to {}, ratio={}", this, tableSchemaDefaultSize, scanSchemaSize, ratio);
        return newSize;
    }

    @Override
    public NamedReference[] filterAttributes()
    {
        // Allow dynamic filtering over all columns
        return Arrays
                .stream(schema.names())
                .map(FieldReference::column)
                .toArray(NamedReference[]::new);
    }

    public static boolean isInPredicate(Predicate p)
    {
        return p.name().equals("IN");
    }

    @Override
    public void filter(Predicate[] filters)
    {
        LOG.warn("{} Applying runtime filters length: {}", this, filters.length);
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} filters: {}", this, Arrays.toString(filters));
        }
        AtomicBoolean alwaysFalse = new AtomicBoolean(false);
        Predicate[] predicates = Arrays.stream(filters).filter(f -> {
            if (isInPredicate(f)) {
                Expression[] children = f.children();
                int length = children.length;
                if (length > filterValueSizeLimit + 1) {
                    LOG.info("{} dropping too large runtime In filter on col {}: size={}", this, children[0], length);
                    return false;
                }
                else if (length == 1) {
                    LOG.info("{} Detected AlwaysFalse runtime filter on col {}: {}", this, children[0], f);
                    alwaysFalse.set(true);
                    return false;
                }
            }
            else if (f instanceof AlwaysFalse) {
                LOG.warn("{} Detected AlwaysFalse runtime filter: {}", this, f);
                alwaysFalse.set(true);
                return false;
            }
            return true;
        }).toArray(Predicate[]::new);
        if (alwaysFalse.get()) {
            pushDownPredicates = getAlwaysFalsePredicates();
            pushDownPredicatesChanged = true;
        }
        else {
            if (predicates.length > 0) {
                VastPredicatePushdown result = VastPredicatePushdown.parse(predicates, schema,
                        ImmutableMap.of(MIN_MAX_FULL_RANGE_ONLY, false,
                                DYNAMIC_FILTER_COMPACTION_THRESHOLD, runtimeFiltersCompactThreshold,
                                MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD, minMaxCompactionMinValuesThreshold));
                pushDownPredicates = Streams.concat(pushDownPredicates.stream(), result.getPushedDown().stream())
                        .distinct()
                        .collect(Collectors.toList());
                pushDownPredicatesChanged = true;
                LOG.info("{} Formatted new predicates: {}", this, this.formatPredicates());
            }
            else {
                LOG.warn("{} All runtime filters were skipped", this);
            }
        }
    }

    @Override
    public String toString()
    {
        return format("[%s:%s:%s]", VastScan.class.getSimpleName(), this.table.name(), this.scanID);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics()
    {
        return new CustomMetric[] {new SplitFetchTimeMetric(), new SplitFetchIdleTimeMetric(), new SplitGetIdleTime(),
                new PageSizeAVG(), new EmptyPartitionsCount(), new EmptyPagesCount()};
    }

    public VastTable getTable()
    {
        return table;
    }

    public boolean hasSelectivePredicate()
    {
        return !pushDownPredicates.isEmpty();
    }
}
