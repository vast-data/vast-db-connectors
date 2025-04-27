/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb.alter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.vastdata.client.VastClient;
import com.vastdata.client.VerifyParam;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_ROW_ID_COLUMN_NAME;
import static java.lang.String.format;
import static spark.sql.catalog.ndb.TypeUtil.NDB_CATALOG_DOES_NOT_SUPPORT_NON_NULL_COLUMNS;
import static spark.sql.catalog.ndb.VastCatalog.PAGE_SIZE;

public class VastTableChangeFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(VastTableChangeFactory.class);

    private static final BiFunction<List<Field>, String[], List<Field>> filterExistingColumns = (existingColumns, testColumnNames) -> {
        Set<String> testColumnNamesSet = Sets.newHashSet(testColumnNames);
        return existingColumns.stream().filter(f -> testColumnNamesSet.contains(f.getName())).collect(Collectors.toList());
    };

    private final Function<List<Field>, Stream<TableColumnLifecycleContext>> streamFieldsAsDropColumnContext;

    private final String tableName;
    private final String schemaName;

    private class ColumnFetcher implements BiFunction<VastClient, VastTransaction, List<Field>> {

        private List<Field> fields;
        @Override
        public List<Field> apply(VastClient vastClient, VastTransaction tx)
        {
            if (fields == null) {
                try {
                    fields = vastClient.listColumns(tx, schemaName, tableName, PAGE_SIZE, Collections.emptyMap());
                }
                catch (VastException e) {
                    throw toRuntime(e);
                }
            }
            return fields;
        }
    }

    static class CompositeTableChange implements VastTableChange
    {
        private final List<VastTableChange> changes;

        private CompositeTableChange(List<VastTableChange> changes) {
            this.changes = changes;
        }

        @Override
        public void accept(VastClient vastClient, VastTransaction vastTransaction)
        {
            changes.forEach(change -> change.accept(vastClient, vastTransaction));
        }
    }


    public VastTableChangeFactory(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.streamFieldsAsDropColumnContext = fields -> fields.stream().map(field -> new TableColumnLifecycleContext(schemaName, tableName, field));
    }

    private String deleteActionToStr(TableChange.DeleteColumn deleteColumn)
    {
        return format("DeleteColumn(name:%s,ifExists:%s)", Arrays.toString(deleteColumn.fieldNames()), deleteColumn.ifExists());
    }

    public VastTableChange compose(TableChange... tableChanges)
            throws VastUserException
    {
        ColumnFetcher columnFetcher = new ColumnFetcher();
        ImmutableList.Builder<VastTableChange> builder = ImmutableList.builder();
        for(TableChange tableChange: tableChanges) {
            if (tableChange instanceof TableChange.DeleteColumn) {
                TableChange.DeleteColumn delete = (TableChange.DeleteColumn) tableChange;
                if (shouldSkipDelete(delete)) {
                    LOG.debug("Skipping drop column action for table change: {}", deleteActionToStr(delete));
                }
                else {
                    LOG.debug("Adding drop column action for table change: {}", deleteActionToStr(delete));
                    builder.add(getDeleteAction(columnFetcher, delete));
                }
            }
            else if (tableChange instanceof TableChange.AddColumn) {
                LOG.debug("Adding add column action for table change: {}", tableChange);
                builder.add(getAddAction((TableChange.AddColumn) tableChange));
            }
            else if (tableChange instanceof TableChange.RenameColumn) {
                LOG.debug("Adding rename column action for table change: {}", tableChange);
                builder.add(getRenameAction((TableChange.RenameColumn) tableChange));
            }
            else {
                throw toRuntime(new VastUserException(format("Unsupported table change: %s", tableChange)));
            }
        }
        return new CompositeTableChange(builder.build());
    }

    private boolean shouldSkipDelete(TableChange.DeleteColumn delete)
            throws VastUserException
    {
        String[] fieldNames = delete.fieldNames();
        VerifyParam.verify(fieldNames.length == 1, format("Received multiple column names to delete: %s", Arrays.toString(fieldNames)));
        String name = fieldNames[0];
        return VASTDB_SPARK_ROW_ID_COLUMN_NAME.equals(name);
    }

    private VastTableChange getRenameAction(TableChange.RenameColumn rename)
            throws VastUserException
    {
        String[] oldNames = rename.fieldNames();
        VerifyParam.verify(oldNames.length == 1, "Received multiple column names to rename");
        String oldName = oldNames[0];
        return (client, tx) -> {
            try {
                client.alterColumn(tx, schemaName, tableName, new AlterColumnContext(oldName, rename.newName(), ImmutableMap.of(), null));
            }
            catch (VastException e) {
                throw toRuntime(e);
            }
        };
    }

    private VastTableChange getDeleteAction(ColumnFetcher columnFetcher, TableChange.DeleteColumn delete)
    {
        Function<List<Field>, List<Field>> listProcessor;
        if (delete.ifExists()) {
            listProcessor = fieldsList -> filterExistingColumns.apply(fieldsList, delete.fieldNames());
        }
        else {
            listProcessor = fieldsList -> filterExistingColumns.andThen(list -> {
                if (list.size() < delete.fieldNames().length) {
                    throw toRuntime(new VastUserException("Some columns do not exist"));
                }
                return list;
            }).apply(fieldsList, delete.fieldNames());
        }
        return (client, tx) -> new DropColumn(columnFetcher.andThen(listProcessor).andThen(streamFieldsAsDropColumnContext).apply(client, tx)).accept(client, tx);
    }

    private VastTableChange getAddAction(TableChange.AddColumn add)
            throws VastUserException
    {
        DataType dataType = add.dataType();
        boolean nullable = add.isNullable();
        String[] fieldNames = add.fieldNames();

        VerifyParam.verify(fieldNames.length == 1, format("Received multiple column names to add: %s", Arrays.toString(fieldNames)));

        if (!nullable) {
            throw new UnsupportedOperationException(NDB_CATALOG_DOES_NOT_SUPPORT_NON_NULL_COLUMNS);
        }
        List<Field> fields = Arrays.stream(add.fieldNames()).map(name -> TypeUtil.sparkFieldToArrowField(name, dataType, nullable, Metadata.empty())).collect(Collectors.toList());
        return (client, tx) -> new AddColumn(streamFieldsAsDropColumnContext.apply(fields)).accept(client, tx);
    }
}
