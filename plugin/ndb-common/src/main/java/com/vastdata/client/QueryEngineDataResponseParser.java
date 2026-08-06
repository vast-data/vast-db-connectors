/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client;

import com.vastdata.client.metrics.DataResponseParserMetrics;
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
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Optional;

import static java.lang.String.format;

public class QueryEngineDataResponseParser<T, C>
{
    private static final Logger LOG = Logger.get(
            QueryEngineDataResponseParser.class);

    private final Schema schema;
    private final RootAllocator allocator;
    private final MessageChannelReader messageReader;
    private final DataResponseParserMetrics metrics;
    private final QueryDataPageBuilder<T, C> pageBuilder;

    public QueryEngineDataResponseParser(Schema schema, InputStream inputStream,
            QueryDataPageBuilder<T, C> pageBuilder)
    {
        this.schema = schema;
        this.allocator = new RootAllocator();
        this.messageReader = new MessageChannelReader(
                new ReadChannel(Channels.newChannel(inputStream)), allocator);
        this.pageBuilder = pageBuilder;
        this.metrics = new DataResponseParserMetrics();
    }

    public T readNextPage()
            throws IOException
    {
        Optional<VectorSchemaRoot> nextBatch = loadNextBatch(); // read next RecordBatch messages (until EOS is received)
        if (nextBatch.isPresent()) {
            VectorSchemaRoot root = nextBatch.get();
            // load & parse new RecordBatch message, and append into a Trino page
            pageBuilder.add(root);
            return pageBuilder.build(metrics);
        }
        return null; // return empty page if no more RecordBatches
    }

    // Taken from ArrowStreamReader#loadNextBatch
    private Optional<VectorSchemaRoot> loadNextBatch()
            throws IOException
    {
        MessageResult result = messageReader.readNext();

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

            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
                    message, bodyBuffer)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "QueryData: loading %d vectors (%s) from %s, body: %s)",
                            schema.getFields().size(), schema, batch,
                            bodyBuffer);
                }
                VectorLoader loader;
                if (batch
                        .getBodyCompression()
                        .equals(NoCompressionCodec.DEFAULT_BODY_COMPRESSION)) {
                    LOG.debug("No compression");
                    loader = new VectorLoader(root,
                            NoCompressionCodec.Factory.INSTANCE);
                }
                else {
                    LOG.debug("Compression : {}", batch.getBodyCompression());
                    loader = new VectorLoader(root,
                            CommonsCompressionFactory.INSTANCE);
                }
                loader.load(batch); // load `root` vectors from batch
            }
            return root;
        }
        else {
            throw new IOException(
                    format("QueryData: Expected RecordBatch but header was %s",
                            headerType));
        }
    }
}
