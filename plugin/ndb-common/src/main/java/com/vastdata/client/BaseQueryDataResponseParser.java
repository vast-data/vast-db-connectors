/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.vastdata.ShapingLogger;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.error.ExceededTotalAllowedBytesPerColumnException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.metrics.DataResponseParserMetrics;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.util.TypeUtils;
import io.airlift.log.Logger;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.error.VastExceptionFactory.hasInterruptException;
import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public abstract class BaseQueryDataResponseParser<T, C>
        implements Iterator<T>
{
    private static final Logger LOG = Logger.get(
            BaseQueryDataResponseParser.class);
    private static final int KEEP_ALIVE_STREAM_ID = -1; // 0xFFFFFFFF
    private static final int COMPLETED_STREAM_ID = -2; // 0xFFFFFFFE
    private static final int FAILED_STREAM_ID = -3; // 0xFFFFFFFD
    private static final ObjectMapper mapper = new ObjectMapper();
    protected final List<Field> serverFields;
    protected final ShapingLoggerFactory shapingLoggerFactory;
    protected final ShapingLogger shapingLogger;
    protected final String traceStr;
    private final RootAllocator allocator;
    private final QueryDataPagination pagination; // updated only on successful parsing
    private final QueryDataPagination.Update paginationUpdate;
    private final long limitTotalRows;
    private final long columnByteLimit;
    private final VastDebugConfig debugConfig;
    private final Map<Integer, SiloStreamParser> parsers;
    private final List<T> pages;
    private final TypeUtils typeUtils;
    protected DataResponseParserMetrics metrics = new DataResponseParserMetrics();
    protected Map<String, Long> serverMetrics = new HashMap<>();
    private Iterator<T> pageIterator;

    public BaseQueryDataResponseParser(ShapingLoggerFactory shapingLoggerFactory,
                                       VastTraceToken traceToken,
                                       List<Field> serverFields,
                                       QueryDataPagination pagination,
                                       Optional<Long> limitTotalRows,
                                       VastDebugConfig debugConfig,
                                       Optional<Long> columnByteLimit)
    {
        this.shapingLoggerFactory = shapingLoggerFactory;
        this.typeUtils = new TypeUtils(shapingLoggerFactory);
        this.traceStr = traceToken.toString();
        this.allocator = new RootAllocator();

        this.serverFields = serverFields;
        this.shapingLogger = shapingLoggerFactory.getInstance(
                BaseQueryDataResponseParser.class, LOG);
        shapingLogger.debug("QueryData(%s) %s init: requested fields=%s",
                traceStr, BaseQueryDataResponseParser.class.getSimpleName(),
                serverFields);
        this.parsers = new HashMap<>();
        this.pages = new ArrayList<>();
        this.pageIterator = ImmutableList.<T>of().iterator();
        this.pagination = pagination;
        this.paginationUpdate = new QueryDataPagination.Update();
        this.limitTotalRows = limitTotalRows.orElse(Long.MAX_VALUE);
        this.columnByteLimit = columnByteLimit.orElse(Long.MAX_VALUE);
        this.debugConfig = debugConfig;
        metrics.incTotalRequests();
    }

    private static int readInt(InputStream in)
            throws IOException
    {
        int len = 4;
        return ByteBuffer
                .wrap(readNBytes(in, len))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
    }

    private static byte[] readNBytes(InputStream is, int len)
            throws IOException
    {
        byte[] buf = new byte[len];
        new DataInputStream(is).readFully(buf, 0, len);
        return buf;
    }

    private static long readLong(InputStream in)
            throws IOException
    {
        int len = 8;
        return ByteBuffer
                .wrap(readNBytes(in, len))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    @Override
    public boolean hasNext()
    {
        return pageIterator.hasNext();
    }

    @Override
    public T next()
    {
        return pageIterator.next();
    }

    // Must be called before iterating the parsed results
    public void parse(InputStream stream)
            throws ExceededTotalAllowedBytesPerColumnException
    {
        parseInternal(stream);
        shapingLogger.debug("page=%s, update=%s", pagination, paginationUpdate);
        pagination.advance(
                paginationUpdate); // updated only if parsing the results has succeeded
    }

    private void parseInternal(InputStream stream)
            throws ExceededTotalAllowedBytesPerColumnException

    {
        try {
            while (true) {
                if (metrics.getTotalPositions() >= limitTotalRows) {
                    if (LOG.isDebugEnabled()) {
                        shapingLogger.info(
                                "QueryData(%s): early exit after reading %s rows (limit=%s)",
                                traceStr, metrics.getTotalPositions(),
                                limitTotalRows);
                    }
                    return;
                }
                int streamId = readInt(stream);
                switch (streamId) {
                    case KEEP_ALIVE_STREAM_ID:
                        // for now, we don't send any payload with the keep-alive messages
                        shapingLogger.debug(
                                "QueryData(%s): skipping keep-alive", traceStr);
                        continue;
                    case COMPLETED_STREAM_ID:
                        // QueryData is completed successfully, no more messages will follow
                        shapingLogger.debug("QueryData(%s): parsing completed",
                                traceStr);
                        if (debugConfig.isEnableServerStatsCollection()) {
                            byte[] allStatsJsonBytes = null;
                            try {
                                allStatsJsonBytes = stream.readAllBytes();
                                serverMetrics = mapper
                                        .readerFor(Map.class)
                                        .readValue(allStatsJsonBytes);
                            }
                            catch (Exception e) {
                                LOG.warn(
                                        "QueryData(%s): failed to parse server stats json %s",
                                        traceStr, allStatsJsonBytes == null ?
                                                "No Stats" :
                                                new String(allStatsJsonBytes,
                                                        Charset.defaultCharset()));
                            }
                        }
                        return;
                    case FAILED_STREAM_ID:
                        String errorCode = Long.toUnsignedString(readLong(stream));
                        int terminationLength = readInt(stream);

                        if (terminationLength != 0) {
                            throw toRuntime(VastExceptionFactory.serverInvalidResponseError(
                                    String.format("Query(%s) received a QUERY_DATA_FAILED_STREAM_ID and in it a non 0 len from server: %d, but error code was: %s",
                                            traceStr,
                                            terminationLength,
                                            errorCode)));
                        }

                        String errorMessage = String.format("Query(%s) failed due to server error: code=%s", traceStr, errorCode);
                        shapingLogger.error(errorMessage);
                        throw toRuntime(VastExceptionFactory.serverException(errorMessage));
                    default:
                        shapingLogger.debug("QueryData(%s): streamId=%d",
                                traceStr, streamId);
                        break;
                }
                // demultiplex the messages and parse them independently
                long start = System.nanoTime();
                try {
                    parsers
                            .computeIfAbsent(streamId,
                                    id -> new SiloStreamParser(shapingLogger,
                                            id, stream))
                            .process();
                    boolean allFinalPage = parsers
                            .values()
                            .stream()
                            .allMatch(p -> p.isFinalPage);
                    if (allFinalPage && pagination.getNumOfSubSplits() == parsers.size()) {
                        return;
                    }
                }
                finally {
                    metrics.addProcessNanos(System.nanoTime() - start);
                }
            }
        }
        catch (IOException e) {
            if (e instanceof EOFException) {
                // will be handled by VastClient#retryIOErrors (in case of HA)
                String msg = format("QueryData(%s): Disconnected: %s", traceStr,
                        e);
                shapingLogger.error(e, "QueryData(%s): Disconnected: %s",
                        traceStr, e);
                throw new UncheckedIOException(msg, e);
            }
            else if (hasInterruptException(e)) {
                Thread.currentThread().interrupt();
                throw toRuntime(e);
            }
            VastIOException vastIOException = ioException(
                    format("QueryData(%s): Failed to parse response: %s",
                            traceStr, e), e);
            shapingLogger.error(e, "QueryData(%s): Failed to parse response: %s", traceStr, e);
            throw toRuntime(vastIOException);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            VastIOException vastIOException = ioException(
                    format("QueryData(%s): parsing interrupted", traceStr), e);
            shapingLogger.error(e, "QueryData(%s): parsing interrupted",
                    traceStr);
            throw toRuntime(vastIOException);
        }
        catch (ExceededTotalAllowedBytesPerColumnException e) {
            throw e;
        }
        catch (Throwable e) {
            shapingLogger.error(e,
                    "QueryData(%s): Caught an unexpected exception during processing of stream %s",
                    traceStr, stream);
            throw e;
        }
        finally {
            pageIterator = pages.iterator();
            parsers.values().forEach(SiloStreamParser::closeIfNeeded);
            long allocated = allocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("QueryData(%s): %d bytes are not freed: %s",
                        traceStr, allocated, allocator.toVerboseString());
                shapingLogger.error("QueryData(%s): %d bytes are not freed: %s",
                        traceStr, allocated, allocator.toVerboseString());
                throw new IllegalStateException(
                        msg); // TODO: consider disabling via config/session
            }
        }
    }

    protected abstract T joinPages(List<T> pages,
            QueryDataPageBuilder<T, C> pageBuilder);

    protected void dropPages(List<T> pages)
    {
    }

    protected abstract QueryDataPageBuilder<T, C> createPageBuilder(
            Schema requestedSchema);

    public boolean isSplitFinished()
    {
        return pagination.isFinished();
    }

    public long getBytesRead()
    {
        return parsers
                .values()
                .stream()
                .mapToLong(SiloStreamParser::getBytesRead)
                .sum();
    }

    public Map<String, Long> getServerMetrics()
    {
        return serverMetrics;
    }

    /**
     * QueryData response contains interleaved messages from multiple workers
     * (running in different silos). Every message is prefixed by a 32-bit ID,
     * allowing us to demultiplex the response into multiple streams. Each
     * stream is handled by a separate instance of the class below.
     */
    public class SiloStreamParser
    {
        protected final int streamId;
        private final InputStream input;
        private final ShapingLogger shapingLogger;
        protected long nextRowId;

        private ImmutableList.Builder<T> columns;
        // incremented after reading a single column
        private int columnCount;
        // reset after reading a single column
        private MessageChannelReader messageReader;
        private ColumnRowTracker columnRowTracker;
        // set after reading first IPC message (which is the Arrow schema)
        private Schema requestedSchema;
        private QueryDataPageBuilder<T, C> pageBuilder;
        private long bytesRead;
        private boolean isFinalPage;

        public SiloStreamParser(ShapingLogger shapingLogger, int streamId,
                InputStream input)
        {
            this.shapingLogger = shapingLogger;
            this.streamId = streamId;
            this.input = input;
            this.columns = ImmutableList.builder();
            this.columnCount = 0;
            this.messageReader = null;
            this.columnRowTracker = null;
            this.requestedSchema = null;
            this.pageBuilder = null;
        }

        public MessageResult readNextMessage()
                throws IOException
        {
            long start = System.nanoTime();
            MessageResult result;
            try {
                result = messageReader.readNext();
            }
            finally {
                metrics.addReadNanos(System.nanoTime() - start);
            }
            return result;
        }

        // Taken from ArrowStreamReader#readSchema
        private void readSchema()
                throws IOException
        {
            MessageResult result = readNextMessage();

            if (result == null) {
                throw new IOException(
                        format("QueryData(%s)(stream=%s): Unexpected end of input. Missing schema.",
                                traceStr, streamId));
            }

            Message message = result.getMessage();
            Schema responseSchema = loadSchemaMessage(message);

            // check that schema types received from the server matches the expected one
            requestedSchema = new Schema(serverFields
                    .subList(columnCount,
                            columnCount + responseSchema.getFields().size())
                    .stream()
                    .map(f -> typeUtils.adaptMapToList(f, Optional.of(
                            format("QueryData(%s)(stream=%s):", traceStr,
                                    streamId))))
                    .collect(Collectors.toList()));
            // Map block will be reconstructed from key/value blocks by QueryDataResponseSchemaConstructor
            shapingLogger.debug(
                    "QueryData(%s)(stream=%s): readSchema actual=%s, requested=%s",
                    traceStr, streamId, responseSchema, requestedSchema);
            for (int i = 0; i < requestedSchema.getFields().size(); ++i) {
                ArrowType expectedFieldType = requestedSchema
                        .getFields()
                        .get(i)
                        .getType();
                Field actualField = responseSchema.getFields().get(i);
                ArrowType actualFieldType = actualField.getType();
                if (actualFieldType.getTypeID() != ArrowType.ArrowTypeID.Timestamp || expectedFieldType.getTypeID() != ArrowType.ArrowTypeID.Timestamp) {
                    verify(actualFieldType.equals(expectedFieldType),
                            "QueryData(%s)(stream=%s): Column %s has type %s != %s expected",
                            traceStr, streamId, i, actualFieldType,
                            expectedFieldType);
                }
                if (expectedFieldType.isComplex()) {
                    List<Field> expectedChildren = requestedSchema
                            .getFields()
                            .get(i)
                            .getChildren();
                    List<Field> actualChildren = actualField.getChildren();
                    shapingLogger.debug(
                            "QueryData(%s)(stream=%s): Column %s expectedType: %s, expectedChildren: %s, actualType: %s, actualChildren: %s",
                            traceStr, streamId, i, expectedFieldType,
                            expectedChildren, actualFieldType, actualChildren);
                    verify(expectedChildren.size() == actualChildren.size(),
                            "QueryData(%s)(stream=%s): Column %s nested schema does not match: %s / %s",
                            traceStr, streamId, i, expectedChildren,
                            actualChildren);
                }
            }
            // use requested fields (instead of responseSchema) since they contain field names
            pageBuilder = createPageBuilder(requestedSchema);
        }

        private Schema loadSchemaMessage(Message message)
                throws IOException
        {
            if (message.headerType() != MessageHeader.Schema) {
                throw new IOException(
                        format("QueryData(%s)(stream=%d): Expected schema but header was %s",
                                traceStr, streamId, message.headerType()));
            }

            Schema responseSchema = MessageSerializer.deserializeSchema(
                    message);
            MetadataV4UnionChecker.checkRead(responseSchema,
                    MetadataVersion.fromFlatbufID(message.version()));
            return responseSchema;
        }

        // Taken from ArrowStreamReader#loadNextBatch
        private Optional<VectorSchemaRoot> loadNextBatch()
                throws IOException
        {
            MessageResult result = readNextMessage();

            // Reached EOS
            if (result == null) {
                return Optional.empty();
            }

            return Optional.of(loadRecordMessage(result));
        }

        private VectorSchemaRoot loadRecordMessage(MessageResult result)
                throws IOException
        {
            Message message = result.getMessage();
            byte headerType = message.headerType();
            if (headerType == MessageHeader.RecordBatch) {
                ArrowBuf bodyBuffer = result.getBodyBuffer();

                // For zero-length batches, need an empty buffer to deserialize the batch
                if (bodyBuffer == null) {
                    bodyBuffer = allocator.getEmpty();
                }

                VectorSchemaRoot root = VectorSchemaRoot.create(requestedSchema,
                        allocator);
                try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
                        message, bodyBuffer)) {
                    if (LOG.isDebugEnabled()) {
                        shapingLogger.debug(
                                "QueryData(%s)(stream=%d): loading %d vectors (%s) from %s, body: %s)",
                                traceStr, streamId,
                                requestedSchema.getFields().size(),
                                requestedSchema, batch, bodyBuffer);
                    }
                    VectorLoader loader;
                    if (batch
                            .getBodyCompression()
                            .equals(NoCompressionCodec.DEFAULT_BODY_COMPRESSION)) {
                        shapingLogger.debug("No compression");
                        loader = new VectorLoader(root,
                                NoCompressionCodec.Factory.INSTANCE);
                    }
                    else {
                        shapingLogger.debug("Compression : {}",
                                batch.getBodyCompression());
                        loader = new VectorLoader(root,
                                CommonsCompressionFactory.INSTANCE);
                    }
                    loader.load(batch); // load `root` vectors from batch
                }
                return root;
            }
            else {
                throw new IOException(
                        format("QueryData(%s)(stream=%d): Expected RecordBatch but header was %s",
                                traceStr, streamId, headerType));
            }
        }

        class ColumnRowTracker
        {
            private final long columnByteLimit;

            private int totalRowCount;

            public ColumnRowTracker(long columnByteLimit)
            {
                this.columnByteLimit = columnByteLimit;

                this.totalRowCount = 0;
            }

            public void registerAppendRows(int rowCount, long readColumnBytesSoFar)
            {
                int previousTotalRowCount = totalRowCount;
                totalRowCount += rowCount;

                if (readColumnBytesSoFar > columnByteLimit) {
                    ExceededTotalAllowedBytesPerColumnException e = new ExceededTotalAllowedBytesPerColumnException(format(
                            "Exceeded total allowed bytes per column: bytesRead so far: %d, bytesLimit: %d, readRows: %s, suggested rows to read: %s",
                            readColumnBytesSoFar, columnByteLimit, totalRowCount,
                            previousTotalRowCount), previousTotalRowCount);
                    throw e;
                }
            }
        }

        // handle a single message from QueryData response
        public void process()
                throws IOException, InterruptedException,
                ExceededTotalAllowedBytesPerColumnException
        {
            nextRowId = readLong(input);
            if (debugConfig.isDisableArrowParsing()) {
                if (Objects.isNull(messageReader)) {
                    messageReader = new MessageChannelReader(
                            new ReadChannel(Channels.newChannel(input)),
                            allocator);
                    columnRowTracker = new ColumnRowTracker(columnByteLimit);
                }
                MessageResult message = readNextMessage();
                shapingLogger.debug(
                        "QueryData(%s)(stream=%d): skipped %d bytes", traceStr,
                        streamId, messageReader.bytesRead());
                if (Objects.isNull(message)) {
                    bytesRead += messageReader.bytesRead();
                    messageReader = null; // end of stream
                    columnRowTracker = null;
                }
                else if (Objects.nonNull(message.getBodyBuffer())) {
                    message.getBodyBuffer().close(); // free memory
                }
                paginationUpdate.advance(streamId, nextRowId);
                return;
            }

            if (Objects.isNull(messageReader)) {
                // initialize a new reader & builder for parsing a new Arrow IPC stream
                shapingLogger.debug(
                        "QueryData(%s)(stream=%d, nextRow=%d): creating a new reader",
                        traceStr, streamId, nextRowId);
                messageReader = new MessageChannelReader(
                        new ReadChannel(Channels.newChannel(input)), allocator);
                columnRowTracker = new ColumnRowTracker(columnByteLimit);
                readSchema(); // read Arrow schema (as the first IPC stream message)
                return;
            }
            Optional<VectorSchemaRoot> nextBatch = loadNextBatch(); // read next RecordBatch messages (until EOS is received)
            if (nextBatch.isPresent()) {
                VectorSchemaRoot root = nextBatch.get();
                // load & parse new RecordBatch message, and append into a Trino page
                if (LOG.isDebugEnabled()) {
                    shapingLogger.debug(
                            "QueryData(%s)(stream=%d, nextRow=%d): read record batch: rows=%d, columns=%d",
                            traceStr, streamId, nextRowId, root.getRowCount(),
                            root.getFieldVectors().size());
                }
                pageBuilder.add(root);
                columnRowTracker.registerAppendRows(root.getRowCount(), messageReader.bytesRead());
                return;
            }
            // Arrow IPC stream is over - send the resulting page
            if (LOG.isDebugEnabled()) {
                shapingLogger
                        .debug("QueryData(%s)(stream=%d, nextRow=%d): reader is done (%d bytes read)",
                                traceStr, streamId, nextRowId, messageReader
                                        .bytesRead());
            }
            bytesRead += messageReader.bytesRead();
            messageReader = null; // intentionally don't close the reader to keep the underlying shared input stream open for other silos/columns
            long start = System.nanoTime();
            T page;
            try {
                page = pageBuilder.build(metrics);
            }
            finally {
                metrics.addBuildNanos(System.nanoTime() - start);
            }
            this.isFinalPage = pageBuilder.isFinalPage();
            pageBuilder.clear(); // deallocate collected Arrow buffers
            columnCount += requestedSchema.getFields().size();
            requestedSchema = null;
            columns.add(page);
            if (LOG.isDebugEnabled()) {
                shapingLogger.debug(
                        "QueryData(%s)(stream=%d, nextRow=%d): columnCount=%s, fields=%s, read partial page: %s",
                        traceStr, streamId, nextRowId, columnCount,
                        serverFields.size(), page);
            }
            verify(columnCount <= serverFields.size(),
                    "QueryData(%s)(stream=%d, nextRow=%d): too many channels %d (%s expected)",
                    traceStr, streamId, nextRowId, columnCount, serverFields);

            boolean finished = (columnCount == serverFields.size()) || isFinalPage;
            if (finished) {
                if (columnCount == serverFields.size() && !isFinalPage) {
                    // all columns are read - join them into a single page
                    page = joinPages(columns.build(), pageBuilder);
                }
                String operation = "skipped";
                if (!debugConfig.isDisablePageQueueing()) {
                    operation = "sent";
                    pages.add(page);
                }
                if (LOG.isDebugEnabled()) {
                    shapingLogger.debug(
                            "QueryData(%s)(stream=%d, nextRow=%d): %s full page: %s",
                            traceStr, streamId, nextRowId, operation, page);
                }
                columns = ImmutableList.builder();
                columnCount = 0;
                paginationUpdate.advance(streamId, nextRowId);
            }
            pageBuilder = null;
        }

        public void closeIfNeeded()
        {
            List<T> remainingColumns = columns.build();
            if (!remainingColumns.isEmpty()) {
                shapingLogger.error(
                        "QueryData(%s)(stream=%s): message reader has %s non-joined columns",
                        traceStr, streamId, remainingColumns.size());
                dropPages(remainingColumns);
                columns = ImmutableList.builder();
            }
            if (Objects.nonNull(messageReader)) {
                shapingLogger.error(
                        "QueryData(%s)(stream=%s): message reader is prematurely closed",
                        traceStr, streamId);
                bytesRead += messageReader.bytesRead();
                messageReader = null;
            }
            if (Objects.nonNull(pageBuilder)) {
                shapingLogger.error(
                        "QueryData(%s)(stream=%s): page builder is prematurely closed",
                        traceStr, streamId);
                pageBuilder.clear(); // deallocates collected Arrow buffers
                pageBuilder = null;
            }
        }

        public long getBytesRead()
        {
            return bytesRead + (messageReader != null ?
                    messageReader.bytesRead() :
                    0);
        }
    }
}
