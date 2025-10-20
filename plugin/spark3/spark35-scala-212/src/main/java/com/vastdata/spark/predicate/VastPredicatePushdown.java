/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.predicate.in.InHandler;
import com.vastdata.spark.predicate.in.InHandlerResultFunctionFactory;
import com.vastdata.spark.predicate.in.ResultFunction;
import com.vastdata.spark.predicate.in.ResultFunctionMethod;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static java.lang.String.format;

public class VastPredicatePushdown
{
    private static final Logger LOG = LoggerFactory.getLogger(VastPredicatePushdown.class);

    public static final String MIN_MAX_FULL_RANGE_ONLY = "min_max_full_range_only";
    private static final ImmutableMap<String, Object> DEFAULT_PARSE_RULES_MAP = ImmutableMap.of(MIN_MAX_FULL_RANGE_ONLY, true);
    private static final ImmutableSet<String> Set_OF_COMPARISON = ImmutableSet.of(">", "=", "<", "<=", ">=", "<>", "!=");
    private static final ImmutableSet<String> SET_OF_BIGGER_COMPARISON = ImmutableSet.of(">", ">=");
    private static final ImmutableSet<String> SET_OF_LESSER_COMPARISON = ImmutableSet.of("<", "<=");
    private static final ImmutableSet<String> SET_OF_NULL_COMPARISON = ImmutableSet.of("IS_NULL", "IS_NOT_NULL");

    // Spark is using AND semantics (i.e. predicate entries are AND-ed together)
    private final List<List<VastPredicate>> pushedDown; // will be performed by Debbie (in VAST)
    private final List<Predicate> postFilter; // will be performed by Spark engine

    public VastPredicatePushdown(List<List<VastPredicate>> pushedDown, List<Predicate> postFilter)
    {
        this.pushedDown = pushedDown;
        this.postFilter = postFilter;
    }

    public List<List<VastPredicate>> getPushedDown()
    {
        return pushedDown;
    }

    public List<Predicate> getPostFilter()
    {
        return postFilter;
    }

    public static VastPredicatePushdown parse(Predicate[] predicates, StructType schema)
    {
        return parse(predicates, schema, DEFAULT_PARSE_RULES_MAP);
    }

    public static VastPredicatePushdown parse(Predicate[] predicates, StructType schema, Map<String, Object> parsingRules)
    {
        long start = System.nanoTime();
        Map<String, StructField> fieldsMap = Arrays.stream(schema.fields())
                .collect(Collectors.toMap(StructField::name, Function.identity()));
        ImmutableList.Builder<List<VastPredicate>> pushedDownBuilder = ImmutableList.builderWithExpectedSize(predicates.length);
        ImmutableList.Builder<Predicate> postFilterBuilder = ImmutableList.builderWithExpectedSize(predicates.length);

        // AND-ed together
        for (Predicate predicate : predicates) {
            LOG.debug("Inspecting predicate: {}", predicate);
            // OR-ed together
            List<Predicate> parsedOrPredicates = parseOr(predicate, schema, parsingRules);
            ImmutableList.Builder<VastPredicate> pushedDownBuilderCurr = ImmutableList.builderWithExpectedSize(parsedOrPredicates.size());
            Optional<NamedReference> result = parseRanges(parsedOrPredicates);
            if (result.isPresent()) {
                NamedReference reference = result.get();
                String[] fieldNames = reference.fieldNames();
                if (fieldNames.length == 1) {
                    StructField field = verifyNotNull(fieldsMap.get(fieldNames[0]), "Missing %s in %s", reference, schema);
                    if (!isNested(field.dataType())) {
                        for (Predicate pred : parsedOrPredicates) {
                            // we can push ORed one by one since they are ANDed together
                            pushedDownBuilderCurr.add(new VastPredicate(pred, reference, field));
                            LOG.debug("pushed predicate: {}", pred);
                        }
                        pushedDownBuilder.add(pushedDownBuilderCurr.build());
                        continue;
                    }
                }
            }
            postFilterBuilder.add(predicate);
            LOG.debug("un-pushed predicate: {}", predicate);
        }
        LOG.debug("Parse took {} ns", System.nanoTime() - start);
        return new VastPredicatePushdown(pushedDownBuilder.build(), postFilterBuilder.build());
    }

    private static boolean isNested(DataType dataType)
    {
        return dataType instanceof ArrayType || dataType instanceof StructType || dataType instanceof MapType;
    }

    static Optional<NamedReference> parseRanges(List<Predicate> predicates)
    {
        Optional<NamedReference> result = Optional.empty(); // all parts must reference the same column
        for (Predicate part : predicates) {
            Optional<NamedReference> range = parseRange(part);
            if (!range.isPresent()) {
                return Optional.empty();
            }
            if (!result.isPresent()) {
                result = range; // keep the first reference
                continue;
            }
            if (!result.equals(range)) {
                return Optional.empty(); // no support for OR between multiple columns
            }
        }
        return result;
    }

    static Optional<NamedReference> parseRange(Predicate predicate)
    {
        Optional<NamedReference> result = parseComparison(predicate, Set_OF_COMPARISON);
        if (result.isPresent()) {
            return result;
        }
        if (predicate.name().equals("AND")) {
            // allow only "x >/>= low AND x </<= high"-like expressions
            Expression[] children = predicate.children();
            if (children.length == 2 && (children[0] instanceof Predicate) && (children[1] instanceof Predicate)) {
                Optional<NamedReference> left = parseComparison((Predicate) children[0], SET_OF_BIGGER_COMPARISON);
                Optional<NamedReference> right = parseComparison((Predicate) children[1], SET_OF_LESSER_COMPARISON);
                if (left.equals(right)) {
                    return left;
                }
            }
            return Optional.empty();
        }
        return parseUnary(predicate, SET_OF_NULL_COMPARISON);
        // TODO: detect & optimize ANDs that cannot be satisfied (e.g. `x = 1 AND x IS NULL`)
    }

    static Optional<NamedReference> parseComparison(Predicate predicate, ImmutableSet<String> supportedOperators)
    {
        if (supportedOperators.contains(predicate.name())) {
            Expression[] children = predicate.children();
            if (children.length == 2 && (children[0] instanceof NamedReference) && (children[1] instanceof LiteralValue)) {
                return Optional.of((NamedReference) children[0]);
            }
        }
        return Optional.empty();
    }

    static Optional<NamedReference> parseUnary(Predicate predicate, Set<String> supportedOperators)
    {
        if (supportedOperators.contains(predicate.name())) {
            Expression[] children = predicate.children();
            if (children.length == 1 && (children[0] instanceof NamedReference)) {
                return Optional.of((NamedReference) children[0]);
            }
        }
        return Optional.empty();
    }

    static List<Predicate> parseOr(Predicate predicate, StructType schema, Map<String, Object> parsingRules)
    {
        ImmutableList.Builder<Predicate> builder = ImmutableList.builderWithExpectedSize(predicate.children().length);
        parseBinaryPredicateArguments(predicate, builder, schema, parsingRules);
        return builder.build();
    }

    static void parseBinaryPredicateArguments(Predicate predicate, ImmutableList.Builder<Predicate> builder, StructType schema,
            Map<String, Object> parsingRules)
    {
        switch (predicate.name()){
            case "OR":
            {
                for (Expression child : predicate.children()) {
                    verify(child instanceof Predicate, "%s argument must be a predicate: %s", predicate.name(), predicate);
                    parseBinaryPredicateArguments((Predicate) child, builder, schema, parsingRules);
                }
                break;
            }
            case "<>":
            case "!=":
            {
                builder.add(new Predicate(">", predicate.children()));
                builder.add(new Predicate("<", predicate.children()));
                break;
            }
            case "NOT":
            {
                // in case of not equal - we will desugar it to '<>' and perform same logic
                verify(predicate.children().length == 1, "'NOT' predicate must contains a child expression");
                if (((Predicate)predicate.children()[0]).name().equals("=")) {
                    Predicate childPred = new Predicate("<>", predicate.children()[0].children());
                    parseBinaryPredicateArguments(childPred, builder, schema, parsingRules);
                }
                break;
            }
            case "IN":
            {
                // in case of "IN" - we will desugar it to ORed equal predicates
                // i.e: 'x in(1,2,3)' -> 'x = 1 OR x = 2 OR x = 3'
                Expression[] children = predicate.children();
                int length = children.length;
                verify(length >= 2,
                        format("'IN' expression must contain a column name and at least a single value: %s", predicate));
                Expression child0 = children[0];
                NamedReference[] references = child0.references();
                verify(references.length == 1,
                        "Expected 'IN' expression to contain a single column reference: {}", Arrays.toString(references));
                String colName = references[0].fieldNames()[0];
                StructField field = schema.apply(colName);
                DataType dataType = field.dataType();
                ResultFunction resultFunction = InHandlerResultFunctionFactory.get(parsingRules, dataType,
                        ResultFunctionMethod.EXPLICIT_VALUES,
                        (Boolean) parsingRules.get(MIN_MAX_FULL_RANGE_ONLY) ? ResultFunctionMethod.MIN_MAX_FULL_RANGE_ONLY : ResultFunctionMethod.MIN_MAX,
                        ResultFunctionMethod.UNSUPPORTED_TYPE_CHECK,
                        ResultFunctionMethod.MINIMAL_VALUE_COUNT_CHECK);
                try {
                    Predicate[] extracted = InHandler.extract(field, children, resultFunction);
                    String valuesStr;
                    if (extracted.length <= 21) {
                        valuesStr = Arrays.toString(extracted);
                    }
                    else {
                        valuesStr = format("[%s, .., %s]", extracted[0], extracted[extracted.length - 1]);
                    }
                    LOG.info("Parsed result for 'IN' filter of {} values: {}", length - 1, valuesStr);
                    builder.add(extracted);
                }
                catch (VastUserException e) {
                    throw new RuntimeException(format("Failed processing 'IN' filter: %s", predicate), e);
                }
                break;
            }
            default:
                builder.add(predicate);
                break;
        }
    }
}
