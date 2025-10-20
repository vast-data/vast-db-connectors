/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.google.common.collect.ImmutableList;
import com.vastdata.trino.VastColumnHandle;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class VastConnectorExpressionPushdown
{
    private static final Logger LOG = Logger.get(VastConnectorExpressionPushdown.class);
    private static final Map<FunctionName, BinaryOperator<Domain>> ACCUMULATORS = Map.of(
            AND_FUNCTION_NAME, Domain::intersect,
            OR_FUNCTION_NAME, Domain::union);

    private final Map<String, VastColumnHandle> assignments;

    public VastConnectorExpressionPushdown(Map<String, ColumnHandle> assignments)
    {
        this.assignments = assignments
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (VastColumnHandle) entry.getValue()));
    }

    public Optional<ComplexPredicate> apply(ConnectorExpression expression)
    {
        Optional<ComplexPredicate> result = parse(expression);
        // TODO(ORION-107695) currently Debbie doesn't supports all OR/AND combinations.
        boolean supported = result.map(VastConnectorExpressionPushdown::isSupported).orElse(false);
        LOG.debug("complex predicate %s is%s supported", result, supported ? "" : " not");
        return supported ? result : Optional.empty();
    }


    private Optional<ComplexPredicate> parse(ConnectorExpression expression)
    {
        if (expression instanceof Call) {
            Call call = (Call) expression;
            Optional<ColumnDomain> range = parseRange(call);
            if (range.isPresent()) {
                return Optional.of(range.orElseThrow());
            }
            FunctionName name = call.getFunctionName();
            if (!(name.equals(AND_FUNCTION_NAME) || name.equals(OR_FUNCTION_NAME))) {
                return Optional.empty();
            }
            if (call.getArguments().size() < 1) {
                return Optional.empty();
            }

            List<ComplexPredicate> predicates = new ArrayList<>(call.getArguments().size());
            for (ConnectorExpression expr : call.getArguments()) {
                Optional<ComplexPredicate> result = parse(expr);
                if (result.isEmpty()) {
                    return Optional.empty();
                }
                predicates.add(result.orElseThrow());
            }

            BinaryOperator<Domain> accumulator = ACCUMULATORS.get(name);
            Map<VastColumnHandle, Domain> domains = new HashMap<>();
            List<ComplexPredicate> nonDomains = new ArrayList<>();
            for (ComplexPredicate predicate : predicates) {
                if (predicate instanceof ColumnDomain) {
                    ColumnDomain columnDomain = (ColumnDomain) predicate;
                    domains.merge(columnDomain.getColumn(), columnDomain.getDomain(), accumulator);
                }
                else {
                    nonDomains.add(predicate);
                }
            }
            ImmutableList.Builder<ComplexPredicate> builder = ImmutableList.builder();
            for (Map.Entry<VastColumnHandle, Domain> entry : domains.entrySet()) {
                builder.add(new ColumnDomain(entry.getKey(), entry.getValue()));
            }
            builder.addAll(nonDomains);
            List<ComplexPredicate> children = builder.build();
            if (children.size() == 1) {
                return Optional.of(children.get(0));
            }
            return Optional.of(new LogicalFunction(name.getName(), builder.build()));
        }
        return Optional.empty();
    }

    private static final Map<FunctionName, BiFunction<Type, Object, Range>> rangeBuilders = Map.of(
            EQUAL_OPERATOR_FUNCTION_NAME, Range::equal,
            GREATER_THAN_OPERATOR_FUNCTION_NAME, Range::greaterThan,
            GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Range::greaterThanOrEqual,
            LESS_THAN_OPERATOR_FUNCTION_NAME, Range::lessThan,
            LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Range::lessThanOrEqual
    );

    private Optional<ColumnDomain> parseRange(Call call)
    {
        FunctionName name = call.getFunctionName();
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() < 1) {
            return Optional.empty();
        }
        if (args.size() == 1 && name.equals(NOT_FUNCTION_NAME)) {
            // `x IS NOT NULL` is represented as `$not($is_null(x))`
            Optional<ComplexPredicate> result = parse(args.get(0));
            if (result.isPresent() && result.orElseThrow() instanceof ColumnDomain columnDomain) {
                return Optional.of(new ColumnDomain(columnDomain.getColumn(), columnDomain.getDomain().complement()));
            }
            return Optional.empty();
        }
        if (!(args.get(0) instanceof Variable)) {
            return Optional.empty();
        }
        Variable variable = (Variable) args.get(0);
        Type type = variable.getType();
        VastColumnHandle column = assignments.get(variable.getName());
        if (isNull(column)) {
            return Optional.empty();
        }
        Function<Range, Optional<ColumnDomain>> fromRange = range -> {
            Domain domain = Domain.create(ValueSet.ofRanges(range), false);
            return Optional.of(new ColumnDomain(column, domain));
        };
        if (args.size() == 1 && name.equals(IS_NULL_FUNCTION_NAME)) {
            return Optional.of(new ColumnDomain(column, Domain.onlyNull(type)));
        }
        // We rely on Trino reordering comparison operators to have the variable first and the constant second:
        // https://github.com/trinodb/trino/blob/fe3aea3e94276a5803b8fb9975c46e1d75f3aa7f/presto-main/src/main/java/io/prestosql/sql/planner/iterative/rule/CanonicalizeExpressionRewriter.java#L54-L58
        if (args.size() == 2 && args.get(1) instanceof Constant) {
            Constant constant = (Constant) args.get(1);
            Object value = constant.getValue();
            if (name.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
                ValueSet valueSet = ValueSet.ofRanges(Range.equal(type, value)).complement();
                return Optional.of(new ColumnDomain(column, Domain.create(valueSet, false)));
            }
            BiFunction<Type, Object, Range> builder = rangeBuilders.get(name);
            if (nonNull(builder)) {
                Range range = builder.apply(type, value);
                return fromRange.apply(range);
            }
        }
        return Optional.empty();
    }

    // TODO(ORION-107695) currently Debbie supports only `OR(domains)`.
    private static boolean isSupported(ComplexPredicate predicate)
    {
        LOG.debug("checking pushdown support: %s", predicate);
        if (predicate instanceof LogicalFunction) {
            LogicalFunction func = (LogicalFunction) predicate;
            if (!func.getName().equals(OR_FUNCTION_NAME.getName())) {
                LOG.debug("unsupported function: %s", func);
                return false;
            }
            for (ComplexPredicate child : func.getChildren()) {
                if (!(child instanceof ColumnDomain)) {
                    LOG.debug("unsupported child: %s", func);
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
