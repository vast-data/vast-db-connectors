/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.vastdata.client.VerifyParam;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.importdata.ImportDataFileMapper;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.error.VastExceptionFactory.userException;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_COLUMN_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getImportDataHiddenColumnIndex;
import static com.vastdata.client.partition.PartitionConstants.ALLOWED_TRANSFORMS;
import static com.vastdata.client.partition.PartitionConstants.TABULAR_HYDRA_METADATA_KEY_COLUMN_INDEX;
import static com.vastdata.client.partition.PartitionConstants.TABULAR_HYDRA_METADATA_KEY_FUNC;
import static com.vastdata.client.partition.PartitionConstants.TABULAR_PARTITION_KEY_TEMPLATE;
import static com.vastdata.client.partition.PartitionConstants.TRANSFORM_ARG;
import static com.vastdata.client.schema.VastMetadataUtils.PARTITIONED_BY_PROPERTY;
import static java.lang.String.format;

public class VastTrinoSchemaAdaptor
{
    private static final Logger LOG = Logger.get(VastTrinoSchemaAdaptor.class);

    public CreateTableContext adaptForCreateTable(ConnectorTableMetadata tableMetadata,
                                                  ObjectMapper mapper)
            throws VastUserException
    {
        SchemaTableName table = tableMetadata.getTable();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        List<Field> fields = columns.stream().peek(columnMetadata ->
        {
            if (columnMetadata
                    .getName()
                    .equals(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
                throw toRuntime(userException(
                        format("Illegal column name for create table: %s",
                                columnMetadata.getName())));
            }
        }).map(new FieldFactory()).collect(Collectors.toList());
        Optional<String> comment = tableMetadata.getComment();
        Map<String, Object> properties = tableMetadata.getProperties();
        Map<String, String> partitionDefs = getPartitionedBy(
                tableMetadata.getProperties(), fields, mapper);
        LOG.info("Partition column metadata for column %s: %s",
                table.getTableName(), partitionDefs);
        return CreateTableContext.create(table.getSchemaName(),
                table.getTableName(), fields, comment, properties,
                partitionDefs, false);
    }

    public TableColumnLifecycleContext adaptForAddColumn(ConnectorTableHandle tableHandle,
                                                         ColumnMetadata column)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        Field field = new FieldFactory().apply(column);
        return new TableColumnLifecycleContext(table.getSchemaName(),
                table.getTableName(), field);
    }

    public TableColumnLifecycleContext adaptForDropColumn(ConnectorTableHandle tableHandle,
                                                          ColumnHandle column)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        VastColumnHandle columnHandle = (VastColumnHandle) column;
        Field field = columnHandle.getField();
        return new TableColumnLifecycleContext(table.getSchemaName(),
                table.getTableName(), field);
    }

    public DropTableContext adaptForDropTable(ConnectorTableHandle tableHandle)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        return new DropTableContext(table.getSchemaName(),
                table.getTableName());
    }

    public ImportDataContext adaptForImportData(List<String> columns,
                                                Map<String, List<String>> filesInfo,
                                                String dest)
    {
        validateColumns(columns);
        String validatedDest = validateDestinationURL(dest);
        List<ImportDataFile> validatedSourceFiles = validateSourceFiles(columns,
                filesInfo);
        return new ImportDataContext(validatedSourceFiles, validatedDest);
    }

    public ImportDataContext adaptForImportData(VastTableHandle table,
                                                Page page,
                                                Schema schema,
                                                BufferAllocator allocator)
    {
        List<Field> fields = schema.getFields();
        final int hiddenColumnIndex = getImportDataHiddenColumnIndex(fields);
        VastRecordBatchBuilder vastRecordBatchBuilder = new VastRecordBatchBuilder(
                schema, allocator);
        // TODO this singleRoVsrs is a temporary fix for a leak by which these
        // vsrs should be held to be released when finished.
        Map<Integer, VectorSchemaRoot> singleRowVsrs = new HashMap<>();
        Function<Integer, VectorSchemaRoot> rowSupplier = i -> singleRowVsrs.computeIfAbsent(
                i, index -> vastRecordBatchBuilder.build(
                        page.getSingleValuePage(index)));

        IntFunction<ImportDataFile> importDataFileIntFunction = new ImportDataFileMapper(
                rowSupplier, hiddenColumnIndex, singleRowVsrs);
        List<ImportDataFile> sourceFiles = IntStream
                .range(0, page.getPositionCount())
                .mapToObj(importDataFileIntFunction)
                .collect(Collectors.toList());
        return new ImportDataContext(sourceFiles, table.getPath());
    }

    private List<ImportDataFile> validateSourceFiles(List<String> columns,
                                                     Map<String, List<String>> filesInfo)
    {
        checkArgument(filesInfo != null && !filesInfo.isEmpty(),
                "Missing source files param");
        return filesInfo.entrySet().stream().map(e ->
        {
            String fileName = e.getKey();
            throwExceptionIfStringNullOrEmpty(fileName, "Invalid file name");
            String[] split = (fileName.startsWith("/") ?
                    fileName.substring(1) :
                    fileName).split("/", 2);
            checkArgument(split.length == 2,
                    "Invalid source file name string format - bucket is not specified");
            List<String> defaultValues = e.getValue();
            checkArgument(defaultValues.size() == columns.size(),
                    format("Default values number doesn't match columns list for file %s: %s",
                            fileName, defaultValues));
            Map<String, String> defaults = Streams
                    .zip(columns.stream(), defaultValues.stream(),
                            Maps::immutableEntry)
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue));
            return new ImportDataFile(split[0], split[1], defaults);
        }).collect(Collectors.toList());
    }

    private void throwExceptionIfStringNullOrEmpty(String string,
                                                   String errorMessage)
    {
        if (Strings.isNullOrEmpty(string)) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private String validateDestinationURL(String dest)
    {
        throwExceptionIfStringNullOrEmpty(dest, "Invalid destination URL");
        return (dest.startsWith("/") ? dest : "/" + dest);
    }

    private void validateColumns(List<String> columns)
    {
        checkArgument(columns != null, "Missing column names list");
        columns.forEach(f -> throwExceptionIfStringNullOrEmpty(f,
                "Invalid column name"));
    }

    public AlterColumnContext adaptForAlterColumn(VastColumnHandle source,
                                                  String target,
                                                  Map<String, String> properties,
                                                  ColumnStatistics stats)
            throws VastUserException
    {
        String name = source.getField().getName();
        if (target != null) {
            VerifyParam.verify(!Strings.isNullOrEmpty(target),
                    "New column name can't be empty");
        }
        return new AlterColumnContext(name, target, properties,
                serializeColumnStatistics(stats));
    }

    private String serializeColumnStatistics(ColumnStatistics stats)
    {
        // TODO: serialize stats
        return null;
    }

    private Map<String, String> getPartitionedBy(Map<String, Object> properties,
                                                 List<Field> fields,
                                                 ObjectMapper mapper)
    {
        List<PartitionColumnMetadata> rawPartitionedBy = (List<PartitionColumnMetadata>) properties.get(
                PARTITIONED_BY_PROPERTY);
        if (rawPartitionedBy == null) {
            return Collections.emptyMap();
        }
        Map<String, Integer> colNameToIndex = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            colNameToIndex.put(fields.get(i).getName(), i);
        }
        Map<String, String> ret = new HashMap<>();
        for (int columnIndex = 0; columnIndex < rawPartitionedBy.size(); columnIndex++) {
            PartitionColumnMetadata pc = rawPartitionedBy.get(columnIndex);
            if (!ALLOWED_TRANSFORMS.contains(
                    pc.getTransform().toLowerCase(Locale.getDefault()))) {
                throw toRuntime(userException(
                        format("Invalid partition column transform '%s' for column '%s'. Allowed transforms are: %s",
                                pc.getTransform(), pc.sourceColumnName,
                                ALLOWED_TRANSFORMS)));
            }
            String key = format(TABULAR_PARTITION_KEY_TEMPLATE, columnIndex);
            Map<String, Object> props = new HashMap<>();
            props.put(TABULAR_HYDRA_METADATA_KEY_COLUMN_INDEX,
                    colNameToIndex.get(pc.sourceColumnName));
            props.put(TABULAR_HYDRA_METADATA_KEY_FUNC, pc.getTransform());
            if (pc.arg != null) {
                props.put(TRANSFORM_ARG, pc.arg);
            }
            try {
                ret.put(key, mapper.writeValueAsString(props));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return ret;
    }

    private static class FieldFactory
            implements Function<ColumnMetadata, Field>
    {
        @Override
        public Field apply(ColumnMetadata columnMetadata)
        {
            Type type = columnMetadata.getType();
            String comment = columnMetadata.getComment();
            Map<String, String> propertiesAsStringsMap = columnMetadata
                    .getProperties()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> e.getValue().toString()));
            if (!Strings.isNullOrEmpty(comment)) {
                propertiesAsStringsMap.put("comment", comment);
            }
            return TypeUtils.convertTrinoTypeToArrowField(type,
                    columnMetadata.getName(), columnMetadata.isNullable());
        }
    }
}
