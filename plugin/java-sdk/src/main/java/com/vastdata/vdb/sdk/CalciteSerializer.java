/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb.sdk;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastExpressionSerializer;
import com.vastdata.client.schema.EnumeratedSchema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.computeir.flatbuf.BinaryLiteral.createBinaryLiteral;
import static org.apache.arrow.computeir.flatbuf.BooleanLiteral.createBooleanLiteral;
import static org.apache.arrow.computeir.flatbuf.DateLiteral.createDateLiteral;
import static org.apache.arrow.computeir.flatbuf.DecimalLiteral.createDecimalLiteral;
import static org.apache.arrow.computeir.flatbuf.Expression.createExpression;
import static org.apache.arrow.computeir.flatbuf.ExpressionImpl.Literal;
import static org.apache.arrow.computeir.flatbuf.Float32Literal.createFloat32Literal;
import static org.apache.arrow.computeir.flatbuf.Int16Literal.createInt16Literal;
import static org.apache.arrow.computeir.flatbuf.Int32Literal.createInt32Literal;
import static org.apache.arrow.computeir.flatbuf.Int64Literal.createInt64Literal;
import static org.apache.arrow.computeir.flatbuf.Int8Literal.createInt8Literal;
import static org.apache.arrow.computeir.flatbuf.Literal.createLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.BinaryLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.BooleanLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.DateLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.DecimalLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.Float32Literal;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.Int16Literal;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.Int32Literal;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.Int64Literal;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.Int8Literal;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.StringLiteral;
import static org.apache.arrow.computeir.flatbuf.LiteralImpl.TimestampLiteral;
import static org.apache.arrow.computeir.flatbuf.StringLiteral.createStringLiteral;
import static org.apache.arrow.computeir.flatbuf.TimestampLiteral.createTimestampLiteral;

class CalciteSerializer
        extends VastExpressionSerializer
{
    private final static Set<SqlKind> SUPPORTED_KINDS = Set.of(
            SqlKind.EQUALS,
            SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN,
            SqlKind.LESS_THAN_OR_EQUAL,
            SqlKind.AND,
            SqlKind.OR,
            SqlKind.IN
    );
    private final EnumeratedSchema schema;
    private final String statement;
    private final SqlParser.Config config;
    private final Map<String, Integer> nameToFieldIndex = new HashMap<>();
    private final Map<String, Field> nameToField = new HashMap<>();

    CalciteSerializer(EnumeratedSchema schema, String statement)
    {
        this.schema = requireNonNull(schema);
        this.statement = requireNonNull(statement);
        this.config = SqlParser.Config.DEFAULT.withQuotedCasing(Casing.UNCHANGED).withUnquotedCasing(Casing.UNCHANGED);
    }

    static String getTableName(String sql)
    {
        try {
            SqlParser.Config config = SqlParser.Config.DEFAULT.withQuotedCasing(Casing.UNCHANGED).withUnquotedCasing(Casing.UNCHANGED);
            SqlSelect node = getSelectNode(SqlParser.create(sql, config).parseQuery());
            SqlIdentifier from = (SqlIdentifier) node.getFrom();
            return from.toString();
        }
        catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int serialize()
    {
        try {
            schema.getSchema().getFields().forEach(field -> {
                this.nameToFieldIndex.put(field.getName(), buildColumn(schema.getBaseFieldIndexByName(field.getName())));
                this.nameToField.put(field.getName(), Field.nullable(null, field.getType()));
            });
            SqlParser parser = SqlParser.create(statement, config);
            SqlNode node = parser.parseQuery();
            SqlSelect selectNode = getSelectNode(node);
            if (selectNode.getWhere() == null) {
                final int columnOffset = buildColumn(0);
                final int validOffset = buildIsValid(columnOffset);

                return buildAnd(buildOr(validOffset));
            }
            TraverseResult traverseResult = traverseAndBuild(selectNode.getWhere());
            int result = traverseResult.offset;
            if (!traverseResult.wrappedWithOr && !traverseResult.wrappedWithAnd) {
                result = buildOr(result);
            }
            if (!traverseResult.wrappedWithAnd) {
                result = buildAnd(result);
            }
            return result;
        }
        catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static SqlSelect getSelectNode(SqlNode node) {
        SqlSelect selectNode;
        if (node instanceof SqlOrderBy orderByNode) {
            selectNode = (SqlSelect) orderByNode.query;
        }
        else if (node instanceof SqlSelect casted) {
            selectNode = casted;
        }
        else {
            throw new UnsupportedOperationException("Only SELECT statements are supported");
        }
        return selectNode;
    }

    Optional<Integer> getLimit()
    {
        try {
            SqlParser parser = SqlParser.create(statement, config);
            SqlNode sqlNode = parser.parseStmt();

            if (!(sqlNode instanceof SqlOrderBy sqlOrderBy)) {
                return Optional.empty();
            }

            SqlNumericLiteral limitLiteral = (SqlNumericLiteral) sqlOrderBy.fetch;
            return Optional.ofNullable(limitLiteral).map(literal -> literal.intValue(true));
        }
        catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    List<String> getProjectedColumns()
    {
        try {
            SqlParser parser = SqlParser.create(statement, config);
            SqlSelect node = getSelectNode(parser.parseQuery());

            SqlIdentifier id = (SqlIdentifier) node.getSelectList().get(0);
            if (!id.isStar()) {
                return node.getSelectList().stream().map(sqlNode -> ((SqlIdentifier) sqlNode).getSimple()).toList();
            }
            return schema.getSchema().getFields().stream().map(Field::getName).toList();
        }
        catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private TraverseResult traverseAndBuild(SqlNode node)
    {
        if (node instanceof SqlBasicCall call) {
            SqlKind kind = call.getOperator().getKind();
            List<SqlNode> operands = call.getOperandList();
            if (kind == SqlKind.IN || kind == SqlKind.NOT_IN) {
                return TraverseResult.wrappedOr(buildInValues(kind, operands.get(0), (SqlNodeList) operands.get(1)));
            }
            else if (kind == SqlKind.OR) {
                int[] offsets = flatten(operands, kind);
                return TraverseResult.wrappedOr(buildOr(offsets));
            }
            else if (kind == SqlKind.AND) {
                int[] offsets = flatten(operands, kind);
                return TraverseResult.wrappedAnd(buildAnd(offsets));
            }
            else if (kind == SqlKind.IS_NULL) {
                validateCall(operands, kind);
                int column = traverseAndBuild(operands.get(0)).offset;
                return TraverseResult.notWrapped(buildIsNull(column));
            }
            else if (kind == SqlKind.IS_NOT_NULL) {
                validateCall(operands, kind);
                int column = traverseAndBuild(operands.get(0)).offset;
                return TraverseResult.notWrapped(buildIsValid(column));
            }

            validateCall(operands, kind);
            int right;
            if (call.getOperandList().get(1) instanceof SqlLiteral) {
                right = buildValue(operands.get(0), operands.get(1));
            }
            else {
                right = traverseAndBuild(operands.get(1)).offset;
            }
            int left = traverseAndBuild(operands.get(0)).offset;
            return switch (kind) {
                case EQUALS -> TraverseResult.notWrapped(buildEqual(left, right));
                case GREATER_THAN -> TraverseResult.notWrapped(buildGreater(left, right, false));
                case GREATER_THAN_OR_EQUAL -> TraverseResult.notWrapped(buildGreater(left, right, true));
                case LESS_THAN -> TraverseResult.notWrapped(buildLess(left, right, false));
                case LESS_THAN_OR_EQUAL -> TraverseResult.notWrapped(buildLess(left, right, true));
                default -> throw new UnsupportedOperationException("Unsupported operator " + kind);
            };
        }
        else if (node instanceof SqlIdentifier id) {
            String columnName = id.getSimple();
            return TraverseResult.notWrapped(nameToFieldIndex.get(columnName));
        }
        throw new UnsupportedOperationException("Unknown node type " + node.getClass());
    }

    private int[] flatten(List<SqlNode> operands, SqlKind kind)
    {
        ImmutableList<SqlNode> nodes = collectNodes(kind, operands);
        if (kind == SqlKind.OR) {
            validateAllPredicatesOnSameColumn(nodes);
        }
        int[] nodesOffset = new int[nodes.size()];
        Arrays.setAll(nodesOffset, i -> {
            int offset = traverseAndBuild(nodes.get(i)).offset;
            if (kind == SqlKind.AND) {
                offset = buildOr(offset);
            }
            return offset;}
        );
        return nodesOffset;
    }

    private void validateAllPredicatesOnSameColumn(ImmutableList<SqlNode> nodes)
    {
        for (SqlNode node : nodes) {
            if (node instanceof SqlBasicCall call) {
                List<SqlNode> operands = call.getOperandList();
                SqlIdentifier sqlIdentifier = (SqlIdentifier) operands.get(0);
                String columnName = sqlIdentifier.getSimple();
                if (!columnName.equals(((SqlIdentifier) ((SqlBasicCall) nodes.get(0)).getOperandList().get(0)).getSimple())) {
                    throw new UnsupportedOperationException("All OR predicates must be on the same column");
                }
            }
            else {
                throw new UnsupportedOperationException("Only basic calls are supported inside OR predicates");
            }
        }
    }

    private static ImmutableList<SqlNode> collectNodes(SqlKind kind, Iterable<? extends SqlNode> nodes)
    {
        if (nodes instanceof Collection && ((Collection<?>) nodes).isEmpty()) {
            return ImmutableList.of();
        }
        else {
            ImmutableList.Builder<SqlNode> nodeBuilder = ImmutableList.builder();

            for(SqlNode node : nodes) {
                addNodes(kind, nodeBuilder, node);
            }
            return nodeBuilder.build();
        }
    }

    private static void addNodes(SqlKind kind, ImmutableList.Builder<SqlNode> nodeBuilder, SqlNode node)
    {
        if (requireNonNull(node.getKind()) == kind) {
            for (SqlNode operand : ((SqlBasicCall) node).getOperandList()) {
                addNodes(kind, nodeBuilder, operand);
            }
        }
        else {
            nodeBuilder.add(node);
        }
    }

    private static void validateCall(List<SqlNode> operands, SqlKind kind)
    {
        int expectedOperands = 2;
        if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
            expectedOperands = 1;
        }
        if (operands.size() != expectedOperands) {
            throw new UnsupportedOperationException("Only binary operators are supported, found " + kind);
        }
        Optional<SqlNode> invalidNode = operands.stream().filter(node -> node instanceof SqlBasicCall).filter(node -> !SUPPORTED_KINDS.contains(node.getKind())).findFirst();
        if (invalidNode.isPresent()) {
            throw new UnsupportedOperationException("Unsupported operand " + invalidNode.get().getKind());
        }
    }

    private int buildInValues(SqlKind kind, SqlNode columnNode, SqlNodeList valuesNode)
    {
        int column = traverseAndBuild(columnNode).offset;
        int[] offsets = new int[valuesNode.size()];
        for (int i = 0; i < valuesNode.size(); i++) {
            if (kind == SqlKind.NOT_IN) {
                offsets[i] = buildNotEqual(column, buildValue(columnNode, valuesNode.get(i)));
            }
            else {
                offsets[i] = buildEqual(column, buildValue(columnNode, valuesNode.get(i)));
            }
        }
        return buildOr(offsets);
    }

    private int buildValue(SqlNode columnNode, SqlNode valueNode)
    {
        SqlIdentifier columnId = (SqlIdentifier) columnNode;
        String columnName = columnId.names.get(0);
        Field field = nameToField.get(columnName);
        if (valueNode instanceof SqlNumericLiteral numericLiteral) {
            return buildSqlNumericalLiteral(numericLiteral, field);
        }
        else if (valueNode instanceof SqlCharStringLiteral sqlCharStringLiteral) {
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Date) {
                String dateAsString = ((NlsString) Objects.requireNonNull(sqlCharStringLiteral.getValue(), "value")).getValue();
                LocalDate date = LocalDate.parse(dateAsString);
                return buildLiteral(
                        DateLiteral,
                        createDateLiteral(builder, date.toEpochDay()),
                        field.getField(builder));
            }
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                String timeAsString = ((NlsString) Objects.requireNonNull(sqlCharStringLiteral.getValue(), "value")).getValue();
                long time;
                try {
                    time = LocalDateTime.parse(timeAsString).toEpochSecond(UTC);
                }
                catch (Exception e) {
                    throw new UnsupportedOperationException("Unsupported time format - valid example 2011-12-03T10:15:30");
                }
                return buildLiteral(
                        TimestampLiteral,
                        createTimestampLiteral(builder, time),
                        field.getField(builder));
            }
            else {
                String value = ((NlsString) Objects.requireNonNull(sqlCharStringLiteral.getValue(), "value")).getValue();
                return buildLiteral(
                        StringLiteral,
                        createStringLiteral(builder, builder.createString(value)),
                        field.getField(builder));
            }
        }
        else if (valueNode instanceof SqlBinaryStringLiteral sqlBinaryStringLiteral) {
            return buildLiteral(
                    BinaryLiteral,
                    createBinaryLiteral(builder, builder.createByteVector(requireNonNull(((BitString) sqlBinaryStringLiteral.getValue())).getAsByteArray())),
                    field.getField(builder));
        }
        else if (valueNode instanceof SqlLiteral sqlLiteral) {
            ArrowType arrowType = field.getType();
            if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Bool) {
                return buildLiteral(
                        BooleanLiteral,
                        createBooleanLiteral(builder, sqlLiteral.booleanValue()),
                        field.getField(builder));
            }
        }
        throw new UnsupportedOperationException("Unsupported value node type " + valueNode.getClass());
    }

    private int buildSqlNumericalLiteral(SqlNumericLiteral numericLiteral, Field field) {
        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Int: {
                ArrowType.Int type = (ArrowType.Int) arrowType;
                switch (type.getBitWidth()) {
                    case 8:
                        return buildLiteral(
                                Int8Literal,
                                createInt8Literal(builder, (byte) numericLiteral.longValue(false)),
                                field.getField(builder));
                    case 16:
                        return buildLiteral(
                                Int16Literal,
                                createInt16Literal(builder, (short) numericLiteral.longValue(false)),
                                field.getField(builder));
                    case 32:
                        return buildLiteral(
                                Int32Literal,
                                createInt32Literal(builder, numericLiteral.intValue(false)),
                                field.getField(builder));
                    case 64:
                        return buildLiteral(
                                Int64Literal,
                                createInt64Literal(builder, numericLiteral.longValue(false)),
                                field.getField(builder));
                    default:
                        throw new UnsupportedOperationException("Unsupported integer bit width " + type.getBitWidth());
                }
            }
            case FloatingPoint: {
                return buildLiteral(
                        Float32Literal,
                        createFloat32Literal(builder, intBitsToFloat(toIntExact(numericLiteral.longValue(false)))),
                        field.getField(builder));
            }
            case Decimal: {
                BigDecimal bigDecimal = numericLiteral.bigDecimalValue();
                byte[] values = bigDecimal.unscaledValue().toByteArray();
                byte[] paddedValues = new byte[16];
                System.arraycopy(values, 0, paddedValues, 0, values.length);
                int valueOffset = org.apache.arrow.computeir.flatbuf.DecimalLiteral.createValueVector(builder, paddedValues);
                return buildLiteral(
                        DecimalLiteral,
                        createDecimalLiteral(builder, valueOffset),
                        field.getField(builder));
            }
            case Date: {
                return buildLiteral(
                        DateLiteral,
                        createDateLiteral(builder, numericLiteral.longValue(false)),
                        field.getField(builder));
            }
        }
        throw new UnsupportedOperationException("Unsupported numeric literal for type " + arrowType);
    }

    private int buildLiteral(Byte literalImpl, int implOffset, int fieldOffset)
    {
        return createExpression(builder, Literal, createLiteral(
                builder,
                literalImpl,
                implOffset,
                fieldOffset));
    }

    private static class TraverseResult
    {
        public final int offset;
        public final boolean wrappedWithOr;
        public final boolean wrappedWithAnd;

        private TraverseResult(int offset, boolean wrappedWithOr, boolean wrappedWithAnd)
        {
            this.offset = offset;
            this.wrappedWithOr = wrappedWithOr;
            this.wrappedWithAnd = wrappedWithAnd;
        }

        private static TraverseResult wrappedOr(int offset)
        {
            return new TraverseResult(offset, true, false);
        }

        private static TraverseResult wrappedAnd(int offset)
        {
            return new TraverseResult(offset, false, true);
        }

        private static TraverseResult notWrapped(int offset)
        {
            return new TraverseResult(offset, false, false);
        }
    }
}
