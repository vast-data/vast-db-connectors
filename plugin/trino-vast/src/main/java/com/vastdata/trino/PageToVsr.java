/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.buffering.VsrAppender;
import com.vastdata.client.metrics.TimeMeasure;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

final class PageToVsr
{
    private PageToVsr()
    {
    }

    public static Map<Long, VsrAppender> getPageToBufferVsrAppender(Page page,
                                                                    BiFunction<Page, Integer, Long> rowBufferAssigner,
                                                                    BufferAllocator allocator,
                                                                   Schema schema)
    {
        return getPageToBufferVsrAppender(List.of(page),
                rowBufferAssigner,
                allocator,
                schema,
                schema.getFields().stream().map(TypeUtils::convertArrowFieldToTrinoType).collect(Collectors.toList()),
                (l) -> {});
    }

    public static Map<Long, VsrAppender> getPageToBufferVsrAppender(
            List<Page> pages,
            BiFunction<Page, Integer, Long> rowBufferAssigner,
            BufferAllocator allocator,
            Schema schema,
            List<Type> types,
            LongConsumer buildVsrTimeConsumer)
    {
        Map<Long, List<Page>> partitionToPages = new HashMap<>();

        for (Page p : pages) {
            if (p.getPositionCount() == 0) {
                continue;
            }
            Map<Long, List<Integer>> partitions = new HashMap<>();
            for (int i = 0; i < p.getPositionCount(); i++) {
                long hash = rowBufferAssigner.apply(p, i);
                partitions.computeIfAbsent(hash, k -> new ArrayList<>()).add(i);
            }
            for (Map.Entry<Long, List<Integer>> partition : partitions.entrySet()) {
                int[] positions = partition.getValue().stream().mapToInt(Integer::intValue).toArray();
                Page slicedPage = p.copyPositions(positions, 0, positions.length);
                partitionToPages.computeIfAbsent(partition.getKey(), k -> new ArrayList<>()).add(slicedPage);
            }
        }

        Map<Long, VsrAppender> bufferIdToAppender = new HashMap<>();
        for (Map.Entry<Long, List<Page>> entry : partitionToPages.entrySet()) {
            bufferIdToAppender.put(entry.getKey(),
                    new MultiVsrPageAppender(entry.getValue(), allocator, schema, types, buildVsrTimeConsumer));
        }

        return bufferIdToAppender;
    }

    static class MultiVsrPageAppender
            implements VsrAppender
    {
        private final List<Page> pages;
        private final BufferAllocator allocator;
        private final Schema schema;
        private final List<Type> types;
        private final LongConsumer buildVsrTimeConsumer;

        public MultiVsrPageAppender(List<Page> pages, BufferAllocator allocator, Schema schema, List<Type> types, LongConsumer buildVsrTimeConsumer)
        {
            this.pages = pages;
            this.allocator = allocator;
            this.schema = schema;
            this.types = types;
            this.buildVsrTimeConsumer = buildVsrTimeConsumer;
        }

        @Override
        public Integer getRowCount()
        {
            return pages.stream().mapToInt(Page::getPositionCount).sum();
        }

        @Override
        public void append(VectorSchemaRoot root)
        {
            if (root.getRowCount() == 0) {
                root.allocateNew();
            }

            if (pages == null || pages.isEmpty()) {
                return;
            }

            Page mergedPage = mergePages(pages);

            VastRecordBatchBuilder recordBatchBuilder = new VastRecordBatchBuilder(schema, allocator);

            TimeMeasure buildVsrTimeMeasure = new TimeMeasure();

            buildVsrTimeMeasure.start();
            try (VectorSchemaRoot tempVsr = recordBatchBuilder.build(mergedPage)) {
                buildVsrTimeMeasure.end(buildVsrTimeConsumer);

                VectorSchemaRootAppender.append(false, root, tempVsr);
            }
        }

        private Page mergePages(List<Page> sourcePages)
        {
            if (sourcePages.size() == 1) {
                return sourcePages.get(0);
            }
            PageBuilder pageBuilder = new PageBuilder(types);

            for (Page page : sourcePages) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    pageBuilder.declarePosition();
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        Block block = page.getBlock(channel);
                        pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
                    }
                }
            }

            // Workaround: PageBuilder.build() often returns blocks with over-allocated internal capacity
            // (e.g., array length 64 for 50 rows). VastRecordBatchBuilder's bulk-copy converters
            // have a bug where they attempt to copy the entire underlying array capacity instead of only
            // the valid positionCount, causing a BufferOverflowException in the strictly-sized Arrow buffers.
            // By using copyPositions with a sequential mapping, we wrap the blocks in DictionaryBlocks,
            // which bypasses the buggy fast-path and forces safe element-by-element extraction.
            //
            // Better Solution: fix the underlying converters
            Page mergedPage = pageBuilder.build();
            int[] positions = new int[mergedPage.getPositionCount()];
            for (int i = 0; i < positions.length; i++) {
                positions[i] = i;
            }
            return mergedPage.copyPositions(positions, 0, positions.length);
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public void close()
        {
        }
    }
}
