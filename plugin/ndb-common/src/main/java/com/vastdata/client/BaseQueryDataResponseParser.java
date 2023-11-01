/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.util.TypeUtils;
import io.airlift.log.Logger;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

abstract public class BaseQueryDataResponseParser<T>
        implements Iterator<T>
{
    private static final Logger LOG = Logger.get(BaseQueryDataResponseParser.class);
    private static final int KEEP_ALIVE_STREAM_ID = -1; // 0xFFFFFFFF
    private static final int COMPLETED_STREAM_ID = -2; // 0xFFFFFFFE

    /**
     * QueryData response contains interleaved messages from multiple workers (running in different silos).
     * Every message is prefixed by a 32-bit ID, allowing us to demultiplex the response into multiple streams.
     * Each stream is handled by a separate instance of the class below.
     */
    public class SiloStreamParser
    {
        private final InputStream input;
        protected final int streamId;
        protected long nextRowId;

        private ImmutableList.Builder<T> columns;
        // incremented after reading a single column
        private int columnCount;
        // reset after reading a single column
        private MessageChannelReader messageReader;
        // set after reading first IPC message (which is the Arrow schema)
        private Schema requestedSchema;
        private QueryDataPageBuilder<T> pageBuilder;

        public SiloStreamParser(int streamId, InputStream input)
        {
            this.streamId = streamId;
            this.input = input;
            this.columns = ImmutableList.builder();
            this.columnCount = 0;
            this.messageReader = null;
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
                readNanos.addAndGet(System.nanoTime() - start);
            }
            return result;
        }

        // Taken from ArrowStreamReader#readSchema
        private void readSchema()
                throws IOException
        {
            MessageResult result = readNextMessage();

            if (result == null) {
                throw new IOException(format("QueryData(%s)(stream=%s): Unexpected end of input. Missing schema.", traceStr, streamId));
            }

            Message message = result.getMessage();
            Schema responseSchema = loadSchemaMessage(message);

            // check that schema types received from the server matches the expected one
            requestedSchema = new Schema(fields.subList(columnCount, columnCount + responseSchema.getFields().size())
                    .stream()
                    .map(f -> TypeUtils.adaptMapToList(f, Optional.of(format("QueryData(%s)(stream=%s):", traceStr, streamId))))
                    .collect(Collectors.toList()));
            // Map block will be reconstructed from key/value blocks by QueryDataResponseSchemaConstructor
            LOG.debug("QueryData(%s)(stream=%s): readSchema actual=%s, requested=%s", traceStr, streamId, responseSchema, requestedSchema);
            for (int i = 0; i < requestedSchema.getFields().size(); ++i) {
                ArrowType expectedFieldType = requestedSchema.getFields().get(i).getType();
                Field actualField = responseSchema.getFields().get(i);
                ArrowType actualFieldType = actualField.getType();
                if (actualFieldType.getTypeID() != ArrowType.ArrowTypeID.Timestamp ||
                        expectedFieldType.getTypeID() != ArrowType.ArrowTypeID.Timestamp) {
                    verify(actualFieldType.equals(expectedFieldType), "QueryData(%s)(stream=%s): Column %s has type %s != %s expected", traceStr, streamId, i, actualFieldType, expectedFieldType);
                }
                if (expectedFieldType.isComplex()) {
                    List<Field> expectedChildren = requestedSchema.getFields().get(i).getChildren();
                    List<Field> actualChildren = actualField.getChildren();
                    verify(expectedChildren.size() == actualChildren.size(), "QueryData(%s)(stream=%s): Column %s nested schema does not match: %s / %s",
                            traceStr, streamId, i, actualChildren, expectedChildren);
                }
            }
            // use requested fields (instead of responseSchema) since they contain field names
            pageBuilder = createPageBuilder(requestedSchema);
        }

        private Schema loadSchemaMessage(Message message)
                throws IOException
        {
            if (message.headerType() != MessageHeader.Schema) {
                throw new IOException(format("QueryData(%s)(stream=%d): Expected schema but header was %s", traceStr, streamId, message.headerType()));
            }

            Schema responseSchema = MessageSerializer.deserializeSchema(message);
            MetadataV4UnionChecker.checkRead(responseSchema, MetadataVersion.fromFlatbufID(message.version()));
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

                VectorSchemaRoot root = VectorSchemaRoot.create(requestedSchema, allocator);
                VectorLoader loader = new VectorLoader(root, NoCompressionCodec.Factory.INSTANCE);
                try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(message, bodyBuffer)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("QueryData(%s)(stream=%d): loading %d vectors (%s) from %s, body: %s)",
                                traceStr, streamId, requestedSchema.getFields().size(), requestedSchema, batch, bodyBuffer);
                    }
                    loader.load(batch); // load `root` vectors from batch
                }
                return root;
            }
            else {
                throw new IOException(format("QueryData(%s)(stream=%d): Expected RecordBatch but header was %s", traceStr, streamId, headerType));
            }
        }

        // handle a single message from QueryData response
        public void process()
                throws IOException, InterruptedException
        {
            nextRowId = readLong(input);
            if (debugConfig.isDisableArrowParsing()) {
                if (Objects.isNull(messageReader)) {
                    messageReader = new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator);
                }
                MessageResult message = readNextMessage();
                LOG.debug("QueryData(%s)(stream=%d): skipped %d bytes", traceStr, streamId, messageReader.bytesRead());
                if (Objects.isNull(message)) {
                    messageReader = null; // end of stream
                }
                else if (Objects.nonNull(message.getBodyBuffer())) {
                    message.getBodyBuffer().close(); // free memory
                }
                paginationUpdate.advance(streamId, nextRowId);
                return;
            }

            if (Objects.isNull(messageReader)) {
                // initialize a new reader & builder for parsing a new Arrow IPC stream
                LOG.debug("QueryData(%s)(stream=%d, nextRow=%d): creating a new reader", traceStr, streamId, nextRowId);
                messageReader = new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator);
                readSchema(); // read Arrow schema (as the first IPC stream message)
                return;
            }
            Optional<VectorSchemaRoot> nextBatch = loadNextBatch(); // read next RecordBatch messages (until EOS is received)
            if (nextBatch.isPresent()) {
                VectorSchemaRoot root = nextBatch.get();
                // load & parse new RecordBatch message, and append into a Trino page
                if (LOG.isDebugEnabled()) {
                    LOG.debug("QueryData(%s)(stream=%d, nextRow=%d): read record batch: rows=%d, columns=%d", traceStr, streamId, nextRowId, root.getRowCount(), root.getFieldVectors().size());
                }
                pageBuilder.add(root);
                return;
            }
            // Arrow IPC stream is over - send the resulting page
            if (LOG.isDebugEnabled()) {
                LOG.debug("QueryData(%s)(stream=%d, nextRow=%d): reader is done (%d bytes read)", traceStr, streamId, nextRowId, messageReader.bytesRead());
            }
            messageReader = null; // intentionally don't close the reader to keep the underlying shared input stream open for other silos/columns
            long start = System.nanoTime();
            T page;
            try {
                page = pageBuilder.build();
            }
            finally {
                buildNanos.addAndGet(System.nanoTime() - start);
            }
            pageBuilder.clear(); // deallocate collected Arrow buffers
            pageBuilder = null;
            columnCount += requestedSchema.getFields().size();
            requestedSchema = null;
            columns.add(page);
            if (LOG.isDebugEnabled()) {
                LOG.debug("QueryData(%s)(stream=%d, nextRow=%d): columnCount=%s, fields=%s, read partial page: %s", traceStr, streamId, nextRowId, columnCount, fields.size(), page);
            }
            verify(columnCount <= fields.size(),
                    "QueryData(%s)(stream=%d, nextRow=%d): too many channels %d (%s expected)", traceStr, streamId, nextRowId, columnCount, fields);
            if (columnCount == fields.size()) {
                // all columns are read - join them into a single page
                page = joinPages(columns.build());
                String operation = "skipped";
                if (!debugConfig.isDisablePageQueueing()) {
                    operation = "sent";
                    pages.add(page);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("QueryData(%s)(stream=%d, nextRow=%d): %s full page: %s", traceStr, streamId, nextRowId, operation, page);
                }
                columns = ImmutableList.builder();
                columnCount = 0;
                paginationUpdate.advance(streamId, nextRowId);
            }
        }

        public void closeIfNeeded()
        {
            List<T> remainingColumns = columns.build();
            if (!remainingColumns.isEmpty()) {
                LOG.error("QueryData(%s)(stream=%s): message reader has %s non-joined columns", traceStr, streamId, remainingColumns.size());
                dropPages(remainingColumns);
                columns = ImmutableList.builder();
            }
            if (Objects.nonNull(messageReader)) {
                LOG.error("QueryData(%s)(stream=%s): message reader is prematurely closed", traceStr, streamId);
                messageReader = null;
            }
            if (Objects.nonNull(pageBuilder)) {
                LOG.error("QueryData(%s)(stream=%s): page builder is prematurely closed", traceStr, streamId);
                pageBuilder.clear(); // deallocates collected Arrow buffers
                pageBuilder = null;
            }
        }
    }

    private final RootAllocator allocator;
    protected final List<Field> fields;

    protected final AtomicLong readNanos = new AtomicLong(0);
    protected final AtomicLong buildNanos = new AtomicLong(0);
    protected final AtomicLong processNanos = new AtomicLong(0);
    protected final AtomicLong totalPositions = new AtomicLong(0);

    private final QueryDataPagination pagination; // updated only on successful parsing
    private final QueryDataPagination.Update paginationUpdate;
    private final long limitTotalRows;
    private final VastDebugConfig debugConfig;

    private final Map<Integer, SiloStreamParser> parsers;
    private final List<T> pages;
    private Iterator<T> pageIterator;
    protected final String traceStr;

    public BaseQueryDataResponseParser(VastTraceToken traceToken, List<Field> fields, QueryDataPagination pagination, Optional<Long> limitTotalRows, VastDebugConfig debugConfig)
    {
        this.traceStr = traceToken.toString();
        this.allocator = new RootAllocator();

        this.fields = fields;
        LOG.info("QueryData(%s) QueryDataResponseParser init: requested fields=%s", traceStr, fields);
        this.parsers = new HashMap<>();
        this.pages = new ArrayList<>();
        this.pageIterator = ImmutableList.<T>of().iterator();
        this.pagination = pagination;
        this.paginationUpdate = new QueryDataPagination.Update();
        this.limitTotalRows = limitTotalRows.orElse(Long.MAX_VALUE);
        this.debugConfig = debugConfig;
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
    {
        parseInternal(stream);
        LOG.debug("page=%s, update=%s", pagination, paginationUpdate);
        pagination.advance(paginationUpdate); // updated only if parsing the results has succeeded
    }
    private void parseInternal(InputStream stream)
    {
        try {
            while (true) {
                if (totalPositions.get() >= limitTotalRows) {
                    if (LOG.isDebugEnabled()) {
                        LOG.info("QueryData(%s): early exit after reading %s rows (limit=%s)", traceStr, totalPositions.get(), limitTotalRows);
                    }
                    return;
                }
                int streamId = readInt(stream);
                switch (streamId) {
                    case KEEP_ALIVE_STREAM_ID:
                        // for now, we don't send any payload with the keep-alive messages
                        LOG.debug("QueryData(%s): skipping keep-alive", traceStr);
                        continue;
                    case COMPLETED_STREAM_ID:
                        // QueryData is completed successfully, no more messages will follow
                        LOG.debug("QueryData(%s): parsing completed", traceStr);
                        return;
                    default:
                        LOG.debug("QueryData(%s): streamId=%d", traceStr, streamId);
                        break;
                }
                // demultiplex the messages and parse them independently
                long start = System.nanoTime();
                try {
                    parsers.computeIfAbsent(streamId, id -> new SiloStreamParser(id, stream)).process();
                }
                finally {
                    processNanos.addAndGet(System.nanoTime() - start);
                }
            }
        }
        catch (IOException e) {
            if (e instanceof EOFException) {
                // will be handled by VastClient#retryIOErrors (in case of HA)
                String msg = format("QueryData(%s): Disconnected: %s", traceStr, e);
                LOG.error(e, msg);
                throw new UncheckedIOException(msg, e);
            }
            VastIOException vastIOException = ioException(format("QueryData(%s): Failed to parse response: %s", traceStr, e), e);
            LOG.error(vastIOException);
            throw toRuntime(vastIOException);
        }
        catch (InterruptedException e) {
            VastIOException vastIOException = ioException(format("QueryData(%s): parsing interrupted", traceStr), e);
            LOG.error(vastIOException);
            throw toRuntime(vastIOException);
        }
        catch (Throwable e) {
            LOG.error(e, "QueryData(%s): Caught an unexpected exception during processing of stream %s", traceStr, stream);
            throw e;
        }
        finally {
            pageIterator = pages.iterator();
            parsers.values().forEach(SiloStreamParser::closeIfNeeded);
            long allocated = allocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("QueryData(%s): %d bytes are not freed: %s", traceStr, allocated, allocator.toVerboseString());
                LOG.error(msg);
                throw new IllegalStateException(msg); // TODO: consider disabling via config/session
            }
        }
    }

    protected abstract T joinPages(List<T> pages);

    protected void dropPages(List<T> pages)
    {
    }

    protected abstract QueryDataPageBuilder<T> createPageBuilder(Schema requestedSchema);

    public boolean isSplitFinished()
    {
        return pagination.isFinished();
    }

    private static int readInt(InputStream in)
            throws IOException
    {
        int len = 4;
        return ByteBuffer.wrap(readNBytes(in, len)).order(ByteOrder.LITTLE_ENDIAN).getInt();
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
        return ByteBuffer.wrap(readNBytes(in, len)).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }


}
