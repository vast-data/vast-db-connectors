/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.vastdata.client.VerifyParam;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.error.VastExceptionFactory.userException;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_COLUMN_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getImportDataHiddenColumnIndex;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTablePath;
import static java.lang.String.format;

public class VastTrinoSchemaAdaptor
{
    public CreateTableContext adaptForCreateTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName table = tableMetadata.getTable();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        List<Field> fields = columns.stream().peek(columnMetadata -> {
            if (columnMetadata.getName().equals(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
                throw toRuntime(userException(format("Illegal column name for create table: %s", columnMetadata.getName())));
            }
        }).map(new FieldFactory()).collect(Collectors.toList());
        Optional<String> comment = tableMetadata.getComment();
        Map<String, Object> properties = tableMetadata.getProperties();
        return new CreateTableContext(table.getSchemaName(), table.getTableName(), fields, comment, properties);
    }

    public TableColumnLifecycleContext adaptForAddColumn(ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        Field field = new FieldFactory().apply(column);
        return new TableColumnLifecycleContext(table.getSchemaName(), table.getTableName(), field);
    }

    public TableColumnLifecycleContext adaptForDropColumn(ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        VastColumnHandle columnHandle = (VastColumnHandle) column;
        Field field = columnHandle.getField();
        return new TableColumnLifecycleContext(table.getSchemaName(), table.getTableName(), field);
    }

    public DropTableContext adaptForDropTable(ConnectorTableHandle tableHandle)
    {
        VastTableHandle table = (VastTableHandle) tableHandle;
        return new DropTableContext(table.getSchemaName(), table.getTableName());
    }

    public ImportDataContext adaptForImportData(List<String> columns, Map<String, List<String>> filesInfo, String dest)
    {
        validateColumns(columns);
        String validatedDest = validateDestinationURL(dest);
        List<ImportDataFile> validatedSourceFiles = validateSourceFiles(columns, filesInfo);
        return new ImportDataContext(validatedSourceFiles, validatedDest);
    }

    public ImportDataContext adaptForImportData(VastTableHandle table, Page page, Schema schema)
    {
        List<Field> fields = schema.getFields();
        final int hiddenColumnIndex = getImportDataHiddenColumnIndex(fields);
        VastRecordBatchBuilder vastRecordBatchBuilder = new VastRecordBatchBuilder(schema);
        List<ImportDataFile> sourceFiles = IntStream.range(0, page.getPositionCount()).mapToObj(i -> {
            Page singleValuePage = page.getSingleValuePage(i);
            VectorSchemaRoot vectorSchemaRoot = vastRecordBatchBuilder.build(singleValuePage);
            // last field is the column name
            VarCharVector parquetPathVector = (VarCharVector) vectorSchemaRoot.getVector(hiddenColumnIndex);
            String fileName = new String(parquetPathVector.get(0), StandardCharsets.UTF_8);
            String[] split = (fileName.startsWith("/") ? fileName.substring(1) : fileName).split("/", 2);
            if (split.length != 2) {
                vectorSchemaRoot.close();
                throw toRuntime(userException("Invalid source file name string format - bucket is not specified"));
            }
            VectorSchemaRoot partitionsOnlyVectorSchemaRoot = vectorSchemaRoot.removeVector(hiddenColumnIndex);
            return new ImportDataFile(split[0], split[1], partitionsOnlyVectorSchemaRoot);
        }).collect(Collectors.toList());
        return new ImportDataContext(sourceFiles, table.getPath());
    }

    private List<ImportDataFile> validateSourceFiles(List<String> columns, Map<String, List<String>> filesInfo)
    {
        checkArgument(filesInfo != null && !filesInfo.isEmpty(), "Missing source files param");
        return filesInfo.entrySet().stream().map(e -> {
            String fileName = e.getKey();
            throwExceptionIfStringNullOrEmpty(fileName, "Invalid file name");
            String[] split = (fileName.startsWith("/") ? fileName.substring(1) : fileName).split("/", 2);
            checkArgument(split.length == 2, "Invalid source file name string format - bucket is not specified");
            List<String> defaultValues = e.getValue();
            checkArgument(defaultValues.size() == columns.size(),
                    format("Default values number doesn't match columns list for file %s: %s", fileName, defaultValues));
            Map<String, String> defaults = Streams.zip(columns.stream(), defaultValues.stream(), Maps::immutableEntry)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new ImportDataFile(split[0], split[1], defaults);
        }).collect(Collectors.toList());
    }

    private void throwExceptionIfStringNullOrEmpty(String string, String errorMessage)
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
        columns.forEach(f -> throwExceptionIfStringNullOrEmpty(f, "Invalid column name"));
    }

    public AlterColumnContext adaptForAlterColumn(VastColumnHandle source, String target, Map<String, String> properties, ColumnStatistics stats)
            throws VastUserException
    {
        String name = source.getField().getName();
        if (target != null) {
            VerifyParam.verify(!Strings.isNullOrEmpty(target), "New column name can't be empty");
        }
        return new AlterColumnContext(name, target, properties, serializeColumnStatistics(stats));
    }

    private String serializeColumnStatistics(ColumnStatistics stats)
    {
        // TODO: serialize stats
        return null;
    }

    private static class FieldFactory
            implements Function<ColumnMetadata, Field>
    {
        @Override
        public Field apply(ColumnMetadata columnMetadata)
        {
            Type type = columnMetadata.getType();
            String comment = columnMetadata.getComment();
            Map<String, String> propertiesAsStringsMap = columnMetadata.getProperties().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
            if (!Strings.isNullOrEmpty(comment)) {
                propertiesAsStringsMap.put("comment", comment);
            }
            return TypeUtils.convertTrinoTypeToArrowField(type, columnMetadata.getName(), columnMetadata.isNullable());
        }
    }
}
