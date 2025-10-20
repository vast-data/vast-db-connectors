package com.vastdata.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.schema.VastViewMetadata;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.*;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.schema.VastViewMetadata.*;
import static com.vastdata.client.schema.VastViewMetadata.PROPERTY_IS_RUN_AS_INVOKER;
import static com.vastdata.trino.TypeUtils.convertArrowFieldToTrinoType;
import static com.vastdata.trino.TypeUtils.convertTrinoTypeToArrowField;
import static com.vastdata.trino.TypeUtils.parseTrinoTypeId;
import static com.vastdata.trino.VastSessionProperties.*;
import static com.vastdata.trino.VastSessionProperties.getEnableSortedProjections;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.*;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class ViewDefinitionHelpers {
    private static final int MAX_PRECISION_INT64 = toIntExact(maxPrecision(8));

    private static final Logger LOG = Logger.get(ViewDefinitionHelpers.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private static long maxPrecision(int numBytes) {
        return Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
    }

    public static VastViewMetadata getVastViewMetadata(ConnectorViewDefinition definition, Map<String, Object> viewProperties, SchemaTableName viewName)
    {
        final Map<String, String> properties = new HashMap<>(viewProperties.size() + 3);
        properties.put(PROPERTY_ORIGINAL_SQL, definition.getOriginalSql());
        definition.getOwner().ifPresent(owner -> properties.put(PROPERTY_OWNER, owner));
        properties.put(PROPERTY_IS_RUN_AS_INVOKER, String.valueOf(definition.isRunAsInvoker()));
        String serializedTrinoTypesIDs;
        try {
            serializedTrinoTypesIDs = OBJECT_MAPPER.writeValueAsString(definition.getColumns());
            properties.put(VAST_TRINO_TYPES_IDS, serializedTrinoTypesIDs);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed serializing Trino TypeIDs", e);
        }
        viewProperties.forEach((key, value) -> {
            if (properties.containsKey(key)) {
                LOG.warn("createView: viewProperties overrides internal Trino property: key = \"%s\", overridden value = \"%s\", new value = \"%s\"", key, properties.get(key), value);
            }
            properties.put(key, value.toString());
        });
        final VectorSchemaRoot metadata = metadata(
                definition.getOriginalSql(),
                definition.getCatalog().orElse(""),
                definition.getComment().orElse(""),
                definition.getPath().stream().map(CatalogSchemaName::getSchemaName).toList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                properties
        );

        final Schema schema = new Schema(definition.getColumns().stream().map(column -> {
                try {
                    return convertTrinoTypeToArrowField(parseTrinoTypeId(column.getType()), column.getName(), true);
                }
                catch (Exception e) {
                    LOG.warn("Defaulting to binary column for unsupported view column: %s", column);
                    return defaultViewColumn(column.getName());
                }
            }
        ).toList());
        return new VastViewMetadata(viewName.getSchemaName(), viewName.getTableName(), metadata, schema);
    }

    public static ConnectorViewDefinition pageToViewDefinition(final Page page, final List<Field> underlyingColumns, final String schemaName)
    {
        final VariableWidthBlock blockSql = (VariableWidthBlock) page.getBlock(indexOf(FIELD_SQL));
        final String sql = blockSql.getSlice(0).toStringUtf8();
        final VariableWidthBlock blockCurrentCatalog = (VariableWidthBlock) page.getBlock(indexOf(FIELD_CURRENT_CATALOG));
        final String currentCatalog = blockCurrentCatalog.getSlice(0).toStringUtf8();
        final VariableWidthBlock blockComment = (VariableWidthBlock) page.getBlock(indexOf(FIELD_COMMENT));
        final String comment = blockComment.getSlice(0).toStringUtf8();
        final VariableWidthBlock blockCurrentNamespace = (VariableWidthBlock) ((ArrayBlock) page.getBlock(indexOf(FIELD_CURRENT_NAMESPACE))).getArray(0);
        final List<String> currentNamespace = IntStream.range(0, blockCurrentNamespace.getPositionCount()).mapToObj(blockCurrentNamespace::getSlice).map(Slice::toStringUtf8).toList();
        final VariableWidthBlock blockQueryColumnNames = (VariableWidthBlock) ((ArrayBlock) page.getBlock(indexOf(FIELD_QUERY_COLUMN_NAMES))).getArray(0);
        final List<String> queryColumnNames = IntStream.range(0, blockQueryColumnNames.getPositionCount()).mapToObj(blockQueryColumnNames::getSlice).map(Slice::toStringUtf8).toList();
        final VariableWidthBlock blockColumnAliases = (VariableWidthBlock) ((ArrayBlock) page.getBlock(indexOf(FIELD_COLUMN_ALIASES))).getArray(0);
        final List<String> columnAliases = IntStream.range(0, blockColumnAliases.getPositionCount()).mapToObj(blockColumnAliases::getSlice).map(Slice::toStringUtf8).toList();
        final VariableWidthBlock blockColumnComments = (VariableWidthBlock) ((ArrayBlock) page.getBlock(indexOf(FIELD_COLUMN_COMMENTS))).getArray(0);
        final List<String> columnComments = IntStream.range(0, blockColumnComments.getPositionCount()).mapToObj(blockColumnComments::getSlice).map(Slice::toStringUtf8).toList();
        final MapBlock blockProperties = (MapBlock) page.getBlock(indexOf(FIELD_PROPERTIES));
        final SqlMap sqlMap = blockProperties.getMap(0);
        final VariableWidthBlock blockKeys = (VariableWidthBlock) sqlMap.getRawKeyBlock();
        final VariableWidthBlock blockValues = (VariableWidthBlock) sqlMap.getRawValueBlock();
        final Map<String, String> properties = new HashMap<>(sqlMap.getSize());
        for (int i = 0; i < sqlMap.getSize(); ++i) {
            final String key = blockKeys.getSlice(i).toStringUtf8();
            final String value = blockValues.getSlice(i).toStringUtf8();
            properties.put(key, value);
        }
        final List<ConnectorViewDefinition.ViewColumn> viewColumns;
        if (properties.containsKey(VAST_TRINO_TYPES_IDS)) {
            String s = properties.get(VAST_TRINO_TYPES_IDS);
            LOG.debug("pageToViewDefinition: Loading view columns from Vast Trino Types IDs property %s: %s", VAST_TRINO_TYPES_IDS, s);
            try {
                viewColumns = OBJECT_MAPPER.readValue(s, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, ConnectorViewDefinition.ViewColumn.class));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(format("Failed parsing VAST_TRINO_TYPES_IDS property string: %s", s), e);
            }
        }
        else {
            LOG.debug("pageToViewDefinition: Loading view columns from underlyingColumns = %s, columnComments = %s", underlyingColumns, columnComments);
            viewColumns = IntStream.range(0, underlyingColumns.size()).mapToObj(i -> {
                final Field column = underlyingColumns.get(i);
                final Type type = convertArrowFieldToTrinoType(column);
                final Optional<String> columnComment = i < columnComments.size() ? Optional.ofNullable(columnComments.get(i)) : Optional.empty();
                return new ConnectorViewDefinition.ViewColumn(column.getName(), type.getTypeId(), columnComment);
            }).toList();
        }
        final String originalSql = properties.getOrDefault(PROPERTY_ORIGINAL_SQL, sql);
        final String owner = properties.getOrDefault(PROPERTY_OWNER, null);
        final boolean isRunAsInvoker = Boolean.parseBoolean(properties.get(PROPERTY_IS_RUN_AS_INVOKER));
        final List<CatalogSchemaName> path = currentNamespace.stream().map(schema -> new CatalogSchemaName(currentCatalog, schema)).toList();
        LOG.warn("Throwing away queryColumnNames = %s, columnAliases = %s, sql = %s", queryColumnNames, columnAliases, sql);
        return new VastConnectorViewDefinition(originalSql, Optional.of(currentCatalog), Optional.of(schemaName),
                viewColumns, Optional.of(comment), Optional.ofNullable(owner), isRunAsInvoker, path, properties);
    }

    public static VastPageSource viewPageSource(final SchemaTableName viewName,
                                                final VastTransactionHandle transactionHandle,
                                                final VastClient client,
                                                final ConnectorSession session)
            throws VastException
    {
        final String trace = "getView:" + viewName.getTableName();
        final VastTraceToken token = transactionHandle.generateTraceToken(Optional.of(trace));
        final Map<String, String> extraQueryParams = ImmutableMap.of("sub-table", VIEW_METADATA_TABLE);
        final List<Field> columns = client.listColumns(transactionHandle, viewName.getSchemaName(), viewName.getTableName(), 1, extraQueryParams);

        final VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(transactionHandle, token, viewName.getSchemaName(), viewName.getTableName());
        final List<URI> endpoints = getDataEndpoints(session);
        final VastSplit split = new VastSplit(endpoints, new VastSplitContext(0, 1, 1, 1), schedulingInfo);
        final QueryDataPagination pagination = new QueryDataPagination(split.getContext().getNumOfSubSplits());
        final List<VastColumnHandle> projectedColumns = columns
                .stream()
                .map(VastColumnHandle::new)
                .collect(Collectors.toList());
        final Set<Field> schemaFields = new LinkedHashSet<>();
        projectedColumns.forEach(vch -> schemaFields.add(vch.getBaseField()));

        LOG.debug("getView schemaFields: %s", schemaFields);
        final EnumeratedSchema enumeratedSchema = new EnumeratedSchema(schemaFields);
        final TrinoProjectionSerializer projectionSerializer = new TrinoProjectionSerializer(projectedColumns, enumeratedSchema);
        final List<Integer> projections = projectionSerializer.getProjectionIndices();
        final Optional<Integer> batchSize = projections.isEmpty() ? Optional.empty() : Optional.of(1);
        final Schema responseSchema = projectionSerializer.getResponseSchema();
        final LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections = projectionSerializer.getBaseFieldWithProjections();
        final TrinoPredicateSerializer predicateSerializer  = new TrinoPredicateSerializer(TupleDomain.all(), Optional.empty(), Collections.emptyList(), enumeratedSchema);
        LOG.debug("getView QueryData(%s) schema: %s, projections: %s, projectedColumns=%s", token, enumeratedSchema, projections, projectedColumns);
        final VastDebugConfig debugConfig = new VastDebugConfig(
                VastSessionProperties.getDebugDisableArrowParsing(session),
                VastSessionProperties.getDebugDisablePageQueueing(session));
        final VastRetryConfig retryConfig = new VastRetryConfig(getRetryMaxCount(session), getRetrySleepDuration(session));
        final Supplier<QueryDataResponseParser> fetchPages = () -> {
            final QueryDataResponseSchemaConstructor querySchema = QueryDataResponseSchemaConstructor.deconstruct(trace, responseSchema, projections, baseFieldWithProjections);
            final AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
            final Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                QueryDataResponseParser parser = new QueryDataResponseParser(token, querySchema, debugConfig, pagination, Optional.of(1L));
                result.set(parser);
                return new QueryDataResponseHandler(parser::parse, token);
            };
            client.queryData(
                    transactionHandle, token, viewName.getSchemaName(), viewName.getTableName(), enumeratedSchema.getSchema(), projectionSerializer, predicateSerializer,
                    handlerSupplier,
                    new AtomicReference<>(),
                    split.getContext(), split.getSchedulingInfo(),
                    endpoints, retryConfig, batchSize, Optional.empty(), pagination,
                    getEnableSortedProjections(session), extraQueryParams);
            return result.get();
        };
        return new VastPageSource(token, split, fetchPages, Optional.of(1L));
    }

    public static VectorSchemaRoot metadata(
            final String sql,
            final String currentCatalog,
            final String comment,
            final List<String> currentNamespace,
            final List<String> queryColumnNames,
            final List<String> columnAliases,
            final List<String> columnComments,
            final Map<String, String> properties)
    {
        final List<Object> values = Arrays.asList(
                sql,
                currentCatalog,
                comment,
                currentNamespace,
                queryColumnNames,
                columnAliases,
                columnComments,
                properties
        );
        final List<Type> types = Arrays.asList(
                VarcharType.VARCHAR,
                VarcharType.VARCHAR,
                VarcharType.VARCHAR,
                new ArrayType(VarcharType.VARCHAR),
                new ArrayType(VarcharType.VARCHAR),
                new ArrayType(VarcharType.VARCHAR),
                new ArrayType(VarcharType.VARCHAR),
                new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators())
        );
        final PageBuilder pageBuilder = new PageBuilder(types);
        for (int i = 0; i < types.size(); ++i) {
            final Type type = types.get(i);
            final BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            final Object value = values.get(i);
            writeValue(type, blockBuilder, value);
        }
        pageBuilder.declarePositions(1);
        return new VastRecordBatchBuilder(VastViewMetadata.VIEW_DETAILS_SCHEMA).build(pageBuilder.build());
    }

    private static void writeValue(final Type type, final BlockBuilder blockBuilder, final Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    type.writeLong(blockBuilder, ((SqlDecimal) value).getUnscaledValue().longValue());
                }
                else if (Decimals.overflows(((SqlDecimal) value).getUnscaledValue(), MAX_PRECISION_INT64)) {
                    type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
                }
                else {
                    type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).getUnscaledValue().longValue()));
                }
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (REAL.equals(type)) {
                float floatValue = ((Number) value).floatValue();
                type.writeLong(blockBuilder, Float.floatToIntBits(floatValue));
            }
            else if (type instanceof VarcharType) {
                Slice slice = truncateToLength(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (type instanceof CharType) {
                Slice slice = truncateToLengthAndTrimSpaces(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer(((SqlVarbinary) value).getBytes()));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIMESTAMP_MILLIS.equals(type) || TIMESTAMP_MICROS.equals(type)) {
                type.writeLong(blockBuilder, ((SqlTimestamp) value).getEpochMicros());
            }
            else if (TIMESTAMP_NANOS.equals(type)) {
                type.writeObject(blockBuilder, new LongTimestamp(((SqlTimestamp) value).getEpochMicros(), ((SqlTimestamp) value).getPicosOfMicros()));
            }
            else {
                switch (type) {
                    case ArrayType _ -> {
                        List<?> array = (List<?>) value;
                        Type elementType = type.getTypeParameters().getFirst();
                        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                            for (Object elementValue : array) {
                                writeValue(elementType, elementBuilder, elementValue);
                            }
                        });
                    }
                    case MapType _ -> {
                        Map<?, ?> map = (Map<?, ?>) value;
                        Type keyType = type.getTypeParameters().get(0);
                        Type valueType = type.getTypeParameters().get(1);
                        ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
                            for (Map.Entry<?, ?> entry : map.entrySet()) {
                                writeValue(keyType, keyBuilder, entry.getKey());
                                writeValue(valueType, valueBuilder, entry.getValue());
                            }
                        });
                    }
                    case RowType _ -> {
                        List<?> array = (List<?>) value;
                        List<Type> fieldTypes = type.getTypeParameters();
                        ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
                            for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                                Type fieldType = fieldTypes.get(fieldId);
                                writeValue(fieldType, fieldBuilders.get(fieldId), array.get(fieldId));
                            }
                        });
                    }
                    case null, default -> throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }
}
