/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.QueryEngineDataResponseParser;
import com.vastdata.client.VastConfig;
import com.vastdata.client.queryengine.DataResponseBatchData;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.trino.VastColumnHandle;
import com.vastdata.trino.VastPageBuilder;
import com.vastdata.trino.VastSessionProperties;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import vastdb.queryengine.protocol.Offset;
import vastdb.queryengine.protocol.QueryId;
import vastdb.queryengine.protocol.Ticket;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.type.StandardTypes.JSON;

public class VastTableFunctionSplitProcessor
        implements TableFunctionSplitProcessor
{
    private static final Logger LOG = Logger.get(
            VastTableFunctionSplitProcessor.class);

    private final VastConfig vastConfig;
    private final ConnectorSession session;
    private final ConnectorSplit split;
    private final VastQueryEngineClient vastClient;
    private final Type jsonType;
    private Offset nextOffset = Offset.newBuilder().setRecordBatchId(0).build();
    private boolean isFinished;

    public VastTableFunctionSplitProcessor(VastConfig vastConfig,
                                           VastQueryEngineClient vastClient,
                                           TypeManager typeManager,
                                           ConnectorSession session,
                                           ConnectorSplit split)
    {
        this.vastConfig = vastConfig;
        this.vastClient = vastClient;
        this.session = session;
        this.split = split;
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
    }

    @VisibleForTesting
    static String timeBySeconds(long timeValueInPico)
    {
        BigInteger picoValue = BigInteger.valueOf(timeValueInPico);
        long totalSeconds = picoValue
                .divide(new BigInteger("1000000000000"))
                .longValue();
        long timeValue = totalSeconds % (24 * 60 * 60L);
        long hours = timeValue / 3600L;
        long minutes = (timeValue % 3600L) / 60L;
        long seconds = timeValue % 60L;
        return wrapWithQuotes(
                String.format("%02d:%02d:%02d", hours, minutes, seconds));
    }

    @VisibleForTesting
    static String timeByMilliSeconds(long timeValueInPico)
    {
        long milliValue = timeValueInPico / 1_000_000L;
        long timeValue = milliValue % (24 * 60 * 60 * 1_000_000L);
        long hours = timeValue / 3_600_000_000L;
        long minutes = (timeValue % 3_600_000_000L) / 60_000_000L;
        long seconds = (timeValue % 60_000_000L) / 1_000_000L;
        long ms = timeValue % 1_000_000L;
        return wrapWithQuotes(
                String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds,
                        ms));
    }

    @VisibleForTesting
    static String timeByMicroSeconds(long timeValueInPico)
    {
        long microValue = timeValueInPico / 1_000_000L;
        long timeValue = microValue % (24 * 60 * 60 * 1_000_000L);
        long hours = timeValue / 3_600_000_000L;
        long minutes = (timeValue % 3_600_000_000L) / 60_000_000L;
        long seconds = (timeValue % 60_000_000L) / 1_000_000L;
        long us = timeValue % 1_000_000L;
        return wrapWithQuotes(
                String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds,
                        us));
    }

    @VisibleForTesting
    static String timeByNanoSeconds(long timeValueInPico)
    {
        long totalNanos = timeValueInPico / 1_000L;
        long timeValue = totalNanos % (24 * 60 * 60 * 1_000_000_000L);
        long hours = timeValue / 3_600_000_000_000L;
        long minutes = (timeValue % 3_600_000_000_000L) / 60_000_000_000L;
        long seconds = (timeValue % 60_000_000_000L) / 1_000_000_000L;
        long ns = timeValue % 1_000_000_000L;
        return wrapWithQuotes(
                String.format("%02d:%02d:%02d.%09d", hours, minutes, seconds,
                        ns));
    }

    private static String wrapWithQuotes(Object value)
    {
        return "\"" + value + "\"";
    }

    @Override
    public TableFunctionProcessorState process()
    {
        VastTableFunctionSplit vastSplit = (VastTableFunctionSplit) split;
        try (RootAllocator allocator = new RootAllocator()) {
            if (isFinished) {
                LOG.info(
                        "VastTableFunctionSplitProcessor is already finished, returning FINISHED");
                return FINISHED;
            }
            QueryId queryId = QueryId.parseFrom(vastSplit.queryId());
            Ticket ticket = Ticket.parseFrom(vastSplit.ticket());
            LOG.info("query id = %s, ticket = %s", queryId, ticket);
            boolean useTicketGlobalEndpoint = VastSessionProperties.getUseTicketGlobalEndpoint(
                    session);
            DataResponseBatchData data = vastClient.getData(
                    useTicketGlobalEndpoint, queryId, ticket, nextOffset);
            if (data.getDataResponse().getEmptyStream()) {
                if (!data.getDataResponse().hasNext()) {
                    vastClient.finishData(queryId, ticket, "query finished");
                    return FINISHED;
                }
                else {
                    LOG.warn(
                            "no data in VastTableFunctionSplitProcessor, finishing");
                    return TableFunctionProcessorState.Processed.produced(
                            SourcePage.create(0).getPage());
                }
            }
            isFinished = !data.getDataResponse().hasNext();
            Schema schema = new ArrowSchemaUtils().parseSchema(
                    vastSplit.schema(), allocator);
            String[] columns = schema
                    .getFields()
                    .stream()
                    .map(Field::getName)
                    .toArray(String[]::new);
            List<BiFunction<Object, Integer, Object>> jsonValueConvertors = createColumnToJsonValueConvertors(
                    schema);
            nextOffset = data.getDataResponse().getNext();
            QueryEngineDataResponseParser<SourcePage, VastColumnHandle> parser = new QueryEngineDataResponseParser<>(
                    schema, data.getInputStream(),
                    new VastPageBuilder(new ShapingLoggerFactory(vastConfig),
                            vastConfig, null, schema));
            SourcePage sourcePage = parser.readNextPage();

            if (sourcePage == null) {
                vastClient.finishData(queryId, ticket,
                        "unable to read next page, finishing VastTableFunctionSplitProcessor");
                return FINISHED;
            }
            return buildResponsePage(columns, jsonValueConvertors, sourcePage);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TableFunctionProcessorState buildResponsePage(String[] columns,
                                                          List<BiFunction<Object, Integer, Object>> jsonValueConvertors,
                                                          SourcePage sourcePage)
    {
        PageBuilder pageBuilder = new PageBuilder(List.of(jsonType));
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        pageBuilder.declarePositions(sourcePage.getPositionCount());
        for (int row = 0; row < sourcePage.getPositionCount(); row++) {
            StringJoiner jsonJoiner = new StringJoiner(",", "{", "}");
            for (int blockIdx = 0; blockIdx < columns.length; blockIdx++) {
                try {
                    Block block = sourcePage.getBlock(blockIdx);
                    Object value = null;
                    if (!block.isNull(row)) {
                        value = jsonValueConvertors
                                .get(blockIdx)
                                .apply(block, row);
                    }
                    jsonJoiner.add("\"" + columns[blockIdx] + "\": " + value);
                }
                catch (Exception e) {
                    LOG.warn(e, "failed to convert row %d, column %s", row,
                            columns[blockIdx]);
                    throw e;
                }
            }
            jsonType.writeSlice(blockBuilder,
                    Slices.utf8Slice(jsonJoiner.toString()));
        }
        return TableFunctionProcessorState.Processed.produced(
                pageBuilder.build());
    }

    private List<BiFunction<Object, Integer, Object>> createColumnToJsonValueConvertors(
            Schema schema)
    {
        return schema
                .getFields()
                .stream()
                .map((this::getFieldConversionFunction))
                .toList();
    }

    /**
     * @param field field the field to create a conversion function for
     * @return a value extractor function for the given type
     */
    private BiFunction<Object, Integer, Object> getFieldConversionFunction(Field field)
    {
        ArrowType arrowType = field.getType();

        switch (field.getType().getTypeID()) {
            case Int:
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getBitWidth() == 32) {
                    return (block, row) ->
                    {
                        IntArrayBlock arrayBlock = (IntArrayBlock) block;
                        return arrayBlock.getInt(row);
                    };
                }
                else if (intType.getBitWidth() == 64) {
                    return (block, row) ->
                    {
                        LongArrayBlock arrayBlock = (LongArrayBlock) block;
                        return arrayBlock.getLong(row);
                    };
                }
                break;
            case FloatingPoint:
                ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
                switch (type.getPrecision().getFlatbufID()) {
                    case Precision.SINGLE:
                        return (block, row) ->
                        {
                            IntArrayBlock arrayBlock = (IntArrayBlock) block;
                            return arrayBlock.getInt(row);
                        };
                    case Precision.DOUBLE:
                        return (block, row) ->
                        {
                            LongArrayBlock arrayBlock = (LongArrayBlock) block;
                            return Double.longBitsToDouble(
                                    arrayBlock.getLong(row));
                        };
                }
                break;
            case Bool:
                return (block, row) ->
                {
                    ByteArrayBlock arrayBlock = (ByteArrayBlock) block;
                    return arrayBlock.getByte(row) != 0;
                };
            case Utf8:
                return (block, row) ->
                {
                    VariableWidthBlock arrayBlock = (VariableWidthBlock) block;
                    return wrapWithQuotes(
                            arrayBlock.getSlice(row).toStringUtf8());
                };
            case Timestamp:
                return (block, row) ->
                {
                    LongArrayBlock arrayBlock = (LongArrayBlock) block;
                    long aLong = arrayBlock.getLong(row);
                    LOG.info("timestamp value: %d", aLong);
                    java.time.Instant instant = java.time.Instant.ofEpochMilli(
                            aLong / 1000);
                    java.time.LocalDateTime dateTime = java.time.LocalDateTime.ofInstant(
                            instant, java.time.ZoneId.systemDefault());
                    return wrapWithQuotes(dateTime.format(
                            java.time.format.DateTimeFormatter.ofPattern(
                                    "yyyy-MM-dd HH:mm:ss")));
                };
            case Date:
                return (block, row) ->
                {
                    IntArrayBlock arrayBlock = (IntArrayBlock) block;
                    return wrapWithQuotes(
                            LocalDate.ofEpochDay(arrayBlock.getInt(row)));
                };
            case Decimal:
                return (block, row) ->
                {
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                    if (decimalType.getPrecision() < 39) {
                        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
                        return Decimals.toString(longArrayBlock.getLong(row),
                                decimalType.getScale());
                    }
                    else {
                        Int128ArrayBlock arrayBlock = (Int128ArrayBlock) block;
                        return arrayBlock.getInt128(row).toString();
                    }
                };
            case List:
            case FixedSizeList:
                return (block, row) ->
                {
                    Field childField = field.getChildren().getFirst();
                    BiFunction<Object, Integer, Object> childConvertionFunction = getFieldConversionFunction(
                            childField);
                    ArrayBlock arrayBlock = (ArrayBlock) block;
                    Block rowBlock = arrayBlock.getArray(row);
                    StringJoiner joiner = new StringJoiner(",", "[", "]");
                    for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                        if (rowBlock.isNull(i)) {
                            joiner.add("null");
                        }
                        else {
                            joiner.add(childConvertionFunction
                                    .apply(rowBlock, i)
                                    .toString());
                        }
                    }
                    return joiner.toString();
                };
            case Time:
                return (block, row) ->
                {
                    TimeUnit unit = ((ArrowType.Time) arrowType).getUnit();
                    LongArrayBlock arrayBlock = (LongArrayBlock) block;
                    long timeValueInPico = arrayBlock.getLong(row);
                    LOG.debug("converting time by unit %s, pico value: %d",
                            unit, timeValueInPico);
                    return switch (unit) {
                        case SECOND -> timeBySeconds(timeValueInPico);
                        case MILLISECOND -> timeByMilliSeconds(timeValueInPico);
                        case MICROSECOND -> timeByMicroSeconds(timeValueInPico);
                        case NANOSECOND -> timeByNanoSeconds(timeValueInPico);
                    };
                };
            case Binary:
            case FixedSizeBinary:
            case Struct:
            case Map:
                break;
        }
        throw new IllegalArgumentException(
                "Unsupported type: " + field.getType());
    }
}
