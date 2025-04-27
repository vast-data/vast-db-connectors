/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import com.vastdata.spark.statistics.FilterEstimator;
import com.vastdata.spark.statistics.TableLevelStatistics;
import ndb.NDB;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
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
        implements Scan, SupportsReportStatistics, SupportsRuntimeFiltering
{
    private static final Logger LOG = LoggerFactory.getLogger(VastScan.class);
    private static final SecureRandom scanIDProvider = new SecureRandom();
    public static final ArrowSchemaUtils ARROW_SCHEMA_UTILS = new ArrowSchemaUtils();
    private final VastTable table;
    private final StructType schema;
    private final Integer limit;
    private final int runtimeFiltersCompactThreshold;
    private final int minMaxCompactionMinValuesThreshold;
    private final int scanBuilderID;
    private final int filterValueSizeLimit;
    private static final AtomicBoolean describeFlag = new AtomicBoolean(false);
    private final boolean verbose = describeFlag.getAndSet(false);

    @VisibleForTesting
    protected List<List<VastPredicate>> pushDownPredicates;

    private Statistics scanStatistics;
    private VastBatch vastBatch;
    private final int scanID = scanIDProvider.nextInt();

    public VastScan(int scanBuilderID, VastTable table, StructType schema, Integer limit, List<List<VastPredicate>> predicates)
            throws VastUserException
    {
        this.scanBuilderID = scanBuilderID;
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
        LOG.info("new VastScan: table={}, scanBuilderID={}, scanID={}", table.name(), this.scanBuilderID, this.scanID);
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

    // Called by Gluten via reflection
    // Will be replace by Substrait-based serialization in the "near" future
    public byte[] serializeComputeIR() {
        String tablePath = "/" + table.name();

        List<Field> projectionFields = TypeUtil.sparkSchemaToArrowFieldsList(schema);
        Schema projectionSchema = new Schema(projectionFields);

        LinkedHashSet<Field> allQueryFields = new LinkedHashSet<>(projectionFields);
        pushDownPredicates
                .stream()
                .flatMap(domain -> domain.stream().map(VastPredicate::getField))
                .map(TypeUtil::sparkFieldToArrowField)
                .forEach(allQueryFields::add);

        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(allQueryFields);

        // FIXME ORION-151464: pass a proper token like it's done in VastColumnarBatchReader
        // FIXME ORION-151464: use proper transaction ID
        VastTraceToken token = new VastTraceToken(Optional.empty(), 0, 0);
        FlatBufferSerializer projections = new SparkProjectionSerializer(projectionSchema, enumeratedSchema);
        FlatBufferSerializer predicate = new SparkPredicateSerializer(token.toString(), pushDownPredicates, enumeratedSchema);
        byte[] blob = ARROW_SCHEMA_UTILS.serializeQueryDataRequestBody(tablePath, enumeratedSchema.getSchema(), projections, predicate);
        LOG.info("Serialized relation={}, predicates={} projections={} into Compute IR: {} bytes",
                 tablePath, pushDownPredicates, projectionFields, blob.length);
        return blob;
    }

    public long getTxId() {
        SimpleVastTransaction tx = toBatch().getOrCreateTx(null);
        return tx != null ? tx.getId() : 0;
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
        LOG.debug("{} toBatch", this);
        if (vastBatch == null) {
            vastBatch = new VastBatch(table, schema, limit, pushDownPredicates);
        }
        else {
            vastBatch.updatePushdownPredicates(pushDownPredicates);
        }
        return vastBatch;
    }

    @Override
    public Statistics estimateStatistics() {
        return this.table.estimateStatistics();
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

    @Override
    public void filter(Filter[] filters)
    {
        LOG.info("{} Applying runtime filters length: {}", this, filters.length);
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} filters: {}", this, Arrays.toString(filters));
        }
        AtomicBoolean alwaysFalse = new AtomicBoolean(false);
        Predicate[] predicates = Arrays.stream(filters).filter(f -> {
            if (f instanceof In) {
                In in = (In) f;
                int length = in.values().length;
                if (length > filterValueSizeLimit) {
                    LOG.info("{} dropping too large runtime In filter on col {}: size={}", this, in.attribute(), length);
                    return false;
                }
                else if (length == 0) {
                    LOG.info("{} Detected AlwaysFalse runtime filter on col {}: {}", this, in.attribute(), in);
                    alwaysFalse.set(true);
                    return false;
                }
            }
            return true;
        }).map(Filter::toV2).toArray(Predicate[]::new);
        if (alwaysFalse.get()) {
            pushDownPredicates = getAlwaysFalsePredicates();
        }
        else {
            VastPredicatePushdown result = VastPredicatePushdown.parse(predicates, schema,
                    ImmutableMap.of(MIN_MAX_FULL_RANGE_ONLY, false,
                            DYNAMIC_FILTER_COMPACTION_THRESHOLD, runtimeFiltersCompactThreshold,
                            MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD, minMaxCompactionMinValuesThreshold));
            pushDownPredicates = Streams.concat(pushDownPredicates.stream(), result.getPushedDown().stream())
                    .distinct()
                    .collect(Collectors.toList());
            LOG.info("{} Formatted new predicates: {}", this, this.formatPredicates());
        }
    }

    @Override
    public String toString()
    {
        return format("[%s:%s:%s]", VastScan.class.getSimpleName(), this.table.name(), this.scanID);
    }

    public VastScanBuilder getScanBuilderForPushDown()
    {
        VastScanBuilder builder = new VastScanBuilder(this.table);
        builder.pruneColumns(schema);
        if (limit != null) {
            builder.pushLimit(limit);
        }
        return builder;
    }

    public boolean hasSelectivePredicate()
    {
        return !pushDownPredicates.isEmpty();
    }
}
