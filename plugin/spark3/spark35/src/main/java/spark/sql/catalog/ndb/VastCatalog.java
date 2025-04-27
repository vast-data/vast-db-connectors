/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.AlterTableContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.DropViewContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.spark.SparkViewMetadata;
import com.vastdata.spark.VastColumnarBatchReader;
import com.vastdata.spark.VastInputPartition;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastTableReadOnly;
import com.vastdata.spark.VastView;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.DefaultSource;
import ndb.NDB;
import ndb.ka.NDBJobsListener;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import spark.sql.catalog.ndb.alter.VastTableChange;
import spark.sql.catalog.ndb.alter.VastTableChangeFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.ParsedURL.PATH_SEPERATOR;
import static com.vastdata.client.ParsedURL.compose;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_FIELD;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_TABLE_NAME_SUFFIX;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.isImportDataTableName;
import static com.vastdata.client.schema.VastViewMetadata.COLUMN_ALIASES_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.COLUMN_COMMENTS_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.COMMENT_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.SQL_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.VIEW_METADATA_TABLE;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_ROW_ID_NONNULL;
import static java.lang.String.format;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.trimTableNameFromRowLevelOpSuffix;

public class VastCatalog
        implements CatalogExtension
{
    public static final String[] EMPTY_NAMESPACE = new String[0];
    private static final Logger LOG = LoggerFactory.getLogger(VastCatalog.class);
    public static final String[] DEFAULT_VAST_CATALOG = {"ndb"};

    private final static DefaultSource defaultVastSource = new DefaultSource();

    public static final int PAGE_SIZE = 1000; // TODO: use setting
    private SparkConfValidator sparkConfValidator;
    private VastClient vastClient;
    private VastTransactionHandleManager<SimpleVastTransaction> transactionsManager;

    private FunctionCatalog functionsCatalogDelegate;

    @Override
    public void initialize(final String name, final CaseInsensitiveStringMap options)
    {
        LOG.debug("initialize {}, {}", name, options);
        try {
            VastConfig config = NDB.getConfig();
            this.vastClient = NDB.getVastClient(config);
            this.transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
            this.functionsCatalogDelegate = new VastNDBFunctionsCatalog();
            this.functionsCatalogDelegate.initialize(name, options);
            Option<SparkContext> active = SparkContext$.MODULE$.getActive();
            boolean empty = active.isEmpty();
            if (!empty) {
                SparkContext sparkContext = active.get();
                SparkConf conf = sparkContext.getConf();
                sparkConfValidator = new SparkConfValidator(conf::getInt, conf::getBoolean);
                Optional<SparkListenerInterface> any = sparkContext.listenerBus().listeners().stream().filter(l -> l instanceof NDBJobsListener).findAny();
                if (!any.isPresent()) {
                    SparkListenerInterface instance = NDBJobsListener.instance(() -> vastClient, config);
                    LOG.info("Registering NDBJobsListener: {}", instance);
                    sparkContext.addSparkListener(instance);
                }
            }
            else {
                throw new RuntimeException("UNEXPECTED CATALOG INIT WITH NO CONTEXT");
            }
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        InitializedVastCatalog.setVastCatalog(this);
        LOG.debug("initialize {}, {}", name, options);
    }

    @Override
    public String name()
    {
        return defaultVastSource.shortName();
    }

    @Override
    public String[] defaultNamespace()
    {
        LOG.debug("defaultNamespace()");
        return DEFAULT_VAST_CATALOG;
    }

    @Override
    public void setDelegateCatalog(CatalogPlugin delegate)
    {
        LOG.debug("setDelegateCatalog()");
    }

    @Override
    public Identifier[] listFunctions(String[] namespace)
            throws NoSuchNamespaceException
    {
        return this.functionsCatalogDelegate.listFunctions(namespace);
    }

    @Override
    public boolean functionExists(Identifier ident)
    {
        return this.functionsCatalogDelegate.functionExists(ident);
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident)
            throws NoSuchFunctionException
    {
        return this.functionsCatalogDelegate.loadFunction(ident);
    }

    @Override
    public String[][] listNamespaces()
    {
        LOG.debug("listNamespaces()");
        try {
            return listNamespaces(EMPTY_NAMESPACE);
        }
        catch (NoSuchNamespaceException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace)
            throws NoSuchNamespaceException
    {
        LOG.debug("listNamespaces({})", Arrays.toString(namespace));
        if (namespace.length == 0 || namespaceExists(namespace)) {
            return VastCatalogUtils.listNamespaces(vastClient, namespace, PAGE_SIZE, transactionsManager);
        }
        else {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public boolean namespaceExists(String[] namespace)
    {
        LOG.debug("namespaceExists({})", Arrays.toString(namespace));
        try {
            if (namespace.length == 1) {
                return vastClient.listBuckets(false).contains(namespace[0]);
            }
            try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
                return vastClient.schemaExists(tx, String.join(PATH_SEPERATOR, namespace));
            }
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
    {
        LOG.debug("loadNamespaceMetadata({})", Arrays.toString(namespace));
        return ImmutableMap.of();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException
    {
        LOG.info("Creating namespace: {}, with metadata: {}", Arrays.toString(namespace), metadata);
        if (namespace.length < 2) {
            throw toRuntime(new VastUserException(format("Namespace identifier must include full schema path: %s", Arrays.toString(namespace))));
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (!vastClient.schemaExists(tx, schemaName)) {
                Map<String, Object> newmap = metadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                vastClient.createSchema(tx, schemaName, new VastMetadataUtils().getPropertiesString(newmap));
            }
            else {
                throw new NamespaceAlreadyExistsException(namespace);
            }
        }
        catch (VastException ve) {
            throw toRuntime(ve);
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException
    {
        throw new UnsupportedOperationException("NDB catalog does not support altering namespaces");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException
    {
        LOG.info("Dropping namespace: {}, cascade: {}", Arrays.toString(namespace), cascade);
        if (cascade) {
            throw new UnsupportedOperationException("NDB catalog does not support drop cascade");
        }
        if (namespace.length < 2) {
            throw toRuntime(new VastUserException(format("Namespace identifier must include full schema path: %s", Arrays.toString(namespace))));
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.schemaExists(tx, schemaName)) {
                vastClient.dropSchema(tx, schemaName);
                return true;
            }
            else {
                throw new NoSuchNamespaceException(namespace);
            }
        }
        catch (VastConflictException vast409) {
            throw new NonEmptyNamespaceException(namespace);
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace)
            throws NoSuchNamespaceException
    {
        LOG.debug("listTables {}", Arrays.toString(namespace));

        if (namespace.length < 2) {
            LOG.warn("Can't list tables without specifying schema");
            throw new NoSuchNamespaceException(namespace);
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (!vastClient.schemaExists(tx, schemaName)) {
                throw new NoSuchNamespaceException(namespace);
            }
            LOG.debug("Listing tables for schema name: {}", schemaName);
            try {
                return vastClient.listTables(tx, schemaName, PAGE_SIZE).map(tableName -> Identifier.of(namespace, tableName)).toArray(Identifier[]::new);
            }
            catch (final VastServerException | VastUserException e) {
                throw toRuntime(e);
            }
        }
        catch (VastException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public boolean tableExists(Identifier ident)
    {
        LOG.debug("tableExists {}", ident);

        String schemaName = compose(ident.namespace());
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            boolean exists = vastClient.tableExists(tx, schemaName, ident.name());
            LOG.debug("tableExists {} return {}", ident, exists);
            return exists;
        }
        catch (final RuntimeException re) {
            throw re;
        }
        catch (final Exception e) {
            throw new RuntimeException(format("Table existence check failed during fetching table info for identifier %s", ident.name()), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident)
            throws NoSuchTableException
    {
        LOG.debug("loadTable {}", ident);
        String schemaName = compose(ident.namespace());
        String tableName = ident.name();
        boolean isImport = false;
        boolean isRowLevelOp = false;
        if (isImportDataTableName(tableName)) {
            isImport = true;
            tableName = getTableNameForAPI(ident.name());
            LOG.debug("loadTable importing into table {}", tableName);
        }
        else if (isForRowLevelOp(tableName)) {
            isRowLevelOp = true;
            tableName = trimTableNameFromRowLevelOpSuffix(ident.name());
            LOG.debug("loadTable row level operation on table {}", tableName);
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            Optional<String> vastTableHandleId = vastClient.getVastTableHandleId(tx, schemaName, tableName);
            if (vastTableHandleId.isPresent()) {
                List<Field> fields = vastClient.listColumns(tx, schemaName, tableName, PAGE_SIZE, Collections.emptyMap());
                if (isImport) {
                    // Adjust schema of the table for only the fields the user mentioned as Spark is doing a strict validation
                    // Add at the end a field for the given imported filename
                    String fieldsList = ident.name().substring(ident.name().indexOf(IMPORT_DATA_TABLE_NAME_SUFFIX) + IMPORT_DATA_TABLE_NAME_SUFFIX.length());
                    if (fieldsList.length() < 2 || fieldsList.charAt(0) != '(' || fieldsList.charAt(fieldsList.length() - 1) != ')' ) {
                         throw toRuntime(new VastUserException(format("Illegal import data field list for table: %s (ident: %s)", tableName, ident.name())));
                    }
                    String[] splitFields = fieldsList.substring(1, fieldsList.length() - 1).split(",");
                    Set<String> givenColumns = new HashSet<>(splitFields.length);
                    for (String str : splitFields) {
                        String trimmed = str.trim();
                        if (!trimmed.isEmpty())
                            givenColumns.add(trimmed);
                    }
                    fields = fields.stream().filter(field -> givenColumns.contains(field.getName())).collect(Collectors.toList());
                    if (fields.size() != givenColumns.size()) {
                        throw toRuntime(new VastUserException(format("Not all given columns exist in the table. ident: %s, fields.size(): %d, givenColumns.size(): %d",
                                ident.name(), fields.size(), givenColumns.size())));
                    }
                    fields.add(IMPORT_DATA_HIDDEN_FIELD);
                }
                else if (isRowLevelOp) {
                    fields = Lists.asList(VASTDB_SPARK_ROW_ID_NONNULL, fields.toArray(new Field[0]));
                }
                StructType schema = TypeUtil.arrowFieldsListToSparkSchema(fields);
                return makeVastTable(schemaName, tableName, vastTableHandleId.get(), schema, () -> this.vastClient, isImport);
            }
            else {
                throw new NoSuchTableException(ident);
            }
        }
        catch (final NoSuchTableException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception e) {
            throw new RuntimeException(format("Loading table failed during fetching table info for identifier %s", ident.name()), e);
        }
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException
    {
        LOG.info("Creating table identifier: {}, schema: {}, partitions: {}, properties: {}", ident, schema, Arrays.toString(partitions), properties);

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();
        if (isImportDataTableName(tableName)) {
            throw toRuntime(new VastUserException(format("Illegal table name for create table: %s", tableName)));
        }

        List<Field> fieldList = TypeUtil.adaptVerifiedSparkSchemaToArrowFieldsList(schema);
        CreateTableContext ctx = new CreateTableContext(schemaName, tableName, fieldList, Optional.empty(), ImmutableMap.of());

        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.schemaExists(tx, schemaName)) {
                if (!vastClient.tableExists(tx, schemaName, tableName)) {
                    vastClient.createTable(tx, ctx);
                    String vastTableHandleId = vastClient.getVastTableHandleId(tx, schemaName, tableName).orElseThrow(() -> VastExceptionFactory.tableHandleIdNotFound(schemaName, tableName));
                    tx.setCommit(true);
                    return makeVastTable(schemaName, tableName, vastTableHandleId, schema, () -> vastClient, false);
                }
                else {
                    throw new TableAlreadyExistsException(ident);
                }
            }
            else {
                throw new NoSuchNamespaceException(ident.namespace());
            }
        }
        catch (final TableAlreadyExistsException | NoSuchNamespaceException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(format("Creating table failed during putting table info to server for identifier %s", ident.name()), any);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes)
            throws NoSuchTableException
    {
        LOG.debug("alterTable {}, {}", ident, Arrays.toString(changes));

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();

        if (isImportDataTableName(tableName)) {
            throw toRuntime(new VastUserException(format("Illegal table name for alter table: %s", tableName)));
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            VastTableChangeFactory vastTableChangeFactory = new VastTableChangeFactory(schemaName, tableName);
            Optional<String> vastTableHandleId = vastClient.getVastTableHandleId(tx, schemaName, tableName);
            if (vastTableHandleId.isPresent()) {
                VastTableChange vastTableChange = vastTableChangeFactory.compose(changes);
                vastTableChange.accept(vastClient, tx);
                tx.setCommit(true);
                List<Field> fields = vastClient.listColumns(tx, schemaName, tableName, PAGE_SIZE, Collections.emptyMap());
                return makeVastTable(schemaName, tableName, vastTableHandleId.get(), TypeUtil.arrowFieldsListToSparkSchema(fields), () -> vastClient, false);
            }
            else {
                throw new NoSuchTableException(ident);
            }
        }
        catch (final NoSuchTableException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(format("Failed applying table changes for identifier %s", ident.name()), any);
        }
    }

    @Override
    public boolean dropTable(Identifier ident)
    {
        LOG.debug("dropTable {}", ident);

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();

        DropTableContext ctx = new DropTableContext(schemaName, tableName);

        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.tableExists(tx, schemaName, tableName)) {
                vastClient.dropTable(tx, ctx);
                tx.setCommit(true);
                return true;
            }
            else {
                return false;
            }
        }
        catch (final RuntimeException re) {
            throw re;
        }
        catch (final Exception e) {
            throw new RuntimeException(format("Failed dropping table for identifier %s", ident.name()), e);
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException
    {
        LOG.info("Renaming table {} to {}", oldIdent, newIdent);
        if (newIdent.namespace().length < 2) {
            throw new RuntimeException(format("Failed renaming table - new name must include valid table path: %s", newIdent));
        }
        String oldBucket = oldIdent.namespace()[0];
        String newBucket = newIdent.namespace()[0];
        if (!oldBucket.equalsIgnoreCase(newBucket)) {
            throw new RuntimeException(format("Failed renaming table - changing bucket is not supported: %s, %s", oldIdent, newIdent));
        }
        String schemaName = compose(oldIdent.namespace());
        String tableName = oldIdent.name();
        String newFullSchemaPath = compose(newIdent.namespace());
        String newSchemaName = compose(Arrays.copyOfRange(newIdent.namespace(), 1, newIdent.namespace().length));
        String newTableName = newIdent.name();

        if (isImportDataTableName(newTableName)) {
            throw toRuntime(new VastUserException(format("Illegal table name for rename table: %s", newTableName)));
        }
        String format = format("%s/%s", newSchemaName, newTableName);
        AlterTableContext ctx = new AlterTableContext(format, null);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (!vastClient.tableExists(tx, schemaName, tableName)) {
                throw new NoSuchTableException(oldIdent);
            }
            if (vastClient.tableExists(tx, newFullSchemaPath, newTableName)) {
                throw new TableAlreadyExistsException(newIdent);
            }
            vastClient.alterTable(tx, schemaName, tableName, ctx);
            tx.setCommit(true);
        }
        catch (final NoSuchTableException | TableAlreadyExistsException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception e) {
            throw new RuntimeException(format("Renaming table failed during update of table info for identifier %s", oldIdent), e);
        }
    }

    private VastTable makeVastTable(String schemaName, String tableName, String handleID, StructType schema,
                                    Supplier<VastClient> clientSupplier, boolean forImportData)
    {
        return sparkConfValidator.writeError
                .map(error -> (VastTable) new VastTableReadOnly(schemaName, tableName, handleID, schema, clientSupplier, forImportData, error))
                .orElseGet(() -> new VastTable(schemaName, tableName, handleID, schema, clientSupplier, forImportData));
    }

    public Identifier[] listViews(String... namespace) throws NoSuchNamespaceException {
        LOG.debug("listViews {}", Arrays.toString(namespace));

        if (namespace.length < 2) {
            LOG.warn("Can't list views without specifying schema");
            throw new NoSuchNamespaceException(namespace);
        }

        final String schemaName = compose(namespace);
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (!vastClient.schemaExists(tx, schemaName)) {
                throw new NoSuchNamespaceException(namespace);
            }
            LOG.debug("Listing views for schema name: {}", schemaName);
            try {
                return vastClient.listViews(tx, schemaName, PAGE_SIZE).map(viewName -> Identifier.of(namespace, viewName)).toArray(Identifier[]::new);
            }
            catch (final VastServerException | VastUserException e) {
                throw toRuntime(e);
            }
        }
        catch (final VastException e) {
            throw toRuntime(e);
        }
    }

    public VastView loadView(final Identifier ident, Optional<VastTransaction> existingTransaction) throws NoSuchViewException
    {
        LOG.debug("loadViewSql {}", ident);
        final String schemaName = compose(ident.namespace());
        final String viewName = ident.name();
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(existingTransaction, vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.schemaExists(tx, schemaName)) {
                if (vastClient.viewExists(tx, schemaName, viewName)) {
                    VastTraceToken token = tx.generateTraceToken(Optional.of(format("getViewMetadata:%s", viewName)));
                    VastConfig config = NDB.getConfig();
                    VastSchedulingInfo schedulingInfo = vastClient.getSchedulingInfo(tx, token, schemaName, viewName);
                    SimpleVastTransaction transaction = new SimpleVastTransaction(tx.getId(), tx.isReadOnly(), false);
                    VastInputPartition partition = new VastInputPartition(null, 0, 0, 1);
                    StructType structType = TypeUtil.arrowFieldsListToSparkSchema(ImmutableList.of(SQL_FIELD, COLUMN_ALIASES_FIELD, COLUMN_COMMENTS_FIELD, COMMENT_FIELD));
                    Map<String, String> extraQueryParams = ImmutableMap.of("sub-table", VIEW_METADATA_TABLE);
                    List<Field> fields = vastClient.listColumns(tx, schemaName, viewName, 1000, ImmutableMap.of());
                    StructType viewSchema = new StructType(fields.stream().map(TypeUtil::arrowFieldToSparkField).toArray(StructField[]::new));
                    try (VastColumnarBatchReader batchReader = new VastColumnarBatchReader(transaction, 0, config,
                            schemaName, viewName, partition, structType, 1, Collections.emptyList(), schedulingInfo, false, extraQueryParams)) {
                        while (batchReader.next()) {
                            ColumnarBatch columnarBatch = batchReader.get();
                            if (columnarBatch.numRows() > 0) {
                                InternalRow row = columnarBatch.getRow(0);
                                String sqlString = row.getUTF8String(0).toString();
                                String[] aliases = rawObjectsArrayToStringsArray(row.getArray(1).array());
                                String[] colComments = rawObjectsArrayToStringsArray(row.getArray(2).array());
                                String comment = row.getString(3);
                                return new VastView(viewName, sqlString, "ndb", comment, ident.namespace(), viewSchema, aliases, aliases, colComments);
                            }
                        }
                        throw new RuntimeException("Failed to load view metadata " + ident);
                    }
                }
                else {
                    throw new NoSuchViewException(ident);
                }
            }
            else {
                throw new NoSuchViewException(ident);
            }
        }
        catch (final NoSuchViewException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(format("Loading view failed for identifier %s", viewName), any);
        }
    }

    private static String[] rawObjectsArrayToStringsArray(Object[] rawAliasArray)
    {
        return rawAliasArray == null ? new String[0] : Arrays.stream(rawAliasArray).map(o -> o == null ? null : o.toString()).toArray(String[]::new);
    }

    public void createView(SparkViewMetadata ctx, boolean replace, Optional<VastTransaction> existingTransaction)
            throws ViewAlreadyExistsException, NoSuchNamespaceException
    {
        LOG.debug("createView: CreateSparkViewContext: {}", ctx);
        final String schemaName = compose(ctx.getIdentifier().namespace());
        final String viewName = ctx.getIdentifier().name();
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(existingTransaction, vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.schemaExists(tx, schemaName)) {
                if (!vastClient.viewExists(tx, schemaName, viewName)) {
                    vastClient.createView(tx, ctx.toVastCreateViewContext());
                }
                else {
                    if (replace) {
                        LOG.debug("createView: replacing existing view");
                        vastClient.dropView(tx, new DropViewContext(schemaName, viewName));
                        vastClient.createView(tx, ctx.toVastCreateViewContext());
                    }
                    else {
                        throw new ViewAlreadyExistsException(ctx.getIdentifier());
                    }
                }
            }
            else {
                throw new NoSuchNamespaceException(ctx.getIdentifier().namespace());
            }
        }
        catch (final ViewAlreadyExistsException | NoSuchNamespaceException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(format("Creating view failed during putting view info to server for identifier %s", viewName), any);
        }
    }

    public boolean dropView(Identifier ident, Optional<VastTransaction> existingTransaction) {
        LOG.debug("dropView {}", ident);

        final String schemaName = compose(ident.namespace());
        final String viewName = ident.name();

        final DropViewContext ctx = new DropViewContext(schemaName, viewName);

        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(existingTransaction, vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            if (vastClient.viewExists(tx, schemaName, viewName)) {
                vastClient.dropView(tx, ctx);
                tx.setCommit(true);
                return true;
            }
            else {
                return false;
            }
        }
        catch (final RuntimeException re) {
            throw re;
        }
        catch (final Exception e) {
            throw new RuntimeException(format("Failed dropping view for identifier %s", ident.name()), e);
        }
    }
}
