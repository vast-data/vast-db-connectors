/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.vastdata.TableLayout;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.partition.PartitionConstants;
import com.vastdata.client.schema.AlterTableContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.DropViewContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.spark.SparkViewMetadata;
import com.vastdata.spark.VastPITTable;
import com.vastdata.spark.VastPartitionedTable;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastView;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.DefaultSource;
import ndb.NDB;
import ndb.NDBJobsListener;
import ndb.NonAcidResolutionRule;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.SparkListenerInterface;
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
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import spark.sql.catalog.ndb.alter.VastTableChange;
import spark.sql.catalog.ndb.alter.VastTableChangeFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.client.partition.PartitionConstants.TABULAR_PARTITION_KEY_TEMPLATE;
import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_DEC128_ROW_ID_NONNULL;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_INT64_ROW_ID_NONNULL;
import static java.lang.String.format;
import static ndb.NDBSparkSessionExtension.getSessionUser;
import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.trimTableNameFromRowLevelOpSuffix;
import static spark.sql.catalog.ndb.VastCatalogUtils.getSortedByColumns;

public class VastCatalog
        implements CatalogExtension
{
    public static final String[] EMPTY_NAMESPACE = new String[0];
    public static final String[] DEFAULT_VAST_CATALOG = {"ndb"};
    public static final int PAGE_SIZE = 1000; // TODO: use setting
    private static final Logger LOG = LoggerFactory.getLogger(
            VastCatalog.class);
    private final static DefaultSource defaultVastSource = new DefaultSource();
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private SparkConfValidator sparkConfValidator;
    private VastClient vastClient;
    private VastTransactionHandleManager<SimpleVastTransaction> transactionsManager;

    private FunctionCatalog functionsCatalogDelegate;
    private VastConfig config;
    private VastCatalogUtils vastCatalogUtils;
    private SparkViewsMetadataReaderFactory sparkViewsMetadataReaderFactory;

    private static Transform tryFromVastPartitionColumn(
            PartitionColumnMetadata metadata)
    {
        switch (metadata.transform) {
            case "Identity":
                return Expressions.identity(metadata.sourceColumnName);
            case "Year":
                return Expressions.years(metadata.sourceColumnName);
            case "Month":
                return Expressions.months(metadata.sourceColumnName);
            case "Day":
                return Expressions.days(metadata.sourceColumnName);
            case "Hour":
                return Expressions.hours(metadata.sourceColumnName);
            case "Bucket":
                return Expressions.bucket(metadata.arg,
                        metadata.sourceColumnName);
            case "Truncate":
                return new NamedTransform("truncate_" + metadata.arg,
                        metadata.sourceColumnName);
            default:
                throw toRuntime(new VastUserException(
                        format("Unsupported partitioning transform: %s",
                                metadata.transform)));
        }
    }

    @Override
    public void initialize(final String name,
            final CaseInsensitiveStringMap options)
    {
        LOG.debug("initialize {}, {}", name, options);
        try {
            this.config = NDB.getConfig();
            this.vastClient = NDB.getVastClient(config);
            this.transactionsManager = VastSparkTransactionsManager.getInstance(
                    vastClient, new VastTransactionFactory());
            this.functionsCatalogDelegate = new VastNDBFunctionsCatalog();
            this.functionsCatalogDelegate.initialize(name, options);
            this.vastCatalogUtils = new VastCatalogUtils(config, vastClient,
                    transactionsManager);
            this.sparkViewsMetadataReaderFactory = new SparkViewsMetadataReaderFactory(
                    config);
            Option<SparkContext> active = SparkContext$.MODULE$.getActive();
            boolean empty = active.isEmpty();
            if (!empty) {
                SparkContext sparkContext = active.get();
                SparkConf conf = sparkContext.getConf();
                sparkConfValidator = new SparkConfValidator(conf::getInt,
                        conf::getBoolean);
                Optional<SparkListenerInterface> any = sparkContext
                        .listenerBus()
                        .listeners()
                        .stream()
                        .filter(l -> l instanceof NDBJobsListener)
                        .findAny();
                if (any.isEmpty()) {
                    SparkListenerInterface instance = NDBJobsListener.instance(
                            () -> vastClient, config);
                    LOG.info("Registering NDBJobsListener: {}", instance);
                    sparkContext.addSparkListener(instance);
                }
            }
            else {
                throw new RuntimeException(
                        "UNEXPECTED CATALOG INIT WITH NO CONTEXT");
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
        final String endUser = getSessionUser(config);
        LOG.debug("listNamespaces({})", Arrays.toString(namespace));
        if (namespace.length == 0 || namespaceExists(namespace)) {
            return vastCatalogUtils.listNamespaces(namespace, PAGE_SIZE,
                    transactionsManager, endUser);
        }
        else {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public boolean namespaceExists(String[] namespace)
    {
        final String endUser = getSessionUser(config);
        LOG.debug("namespaceExists({})", Arrays.toString(namespace));
        try {
            if (namespace.length == 1) {
                return vastClient.listBuckets(false).contains(namespace[0]);
            }
            try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                    transactionsManager,
                    () -> transactionsManager.startTransaction(endUser),
                    endUser)) {
                return vastClient.schemaExists(tx,
                        String.join(PATH_SEPERATOR, namespace), endUser);
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
    public void createNamespace(String[] namespace,
            Map<String, String> metadata)
            throws NamespaceAlreadyExistsException
    {
        final String endUser = getSessionUser(config);
        LOG.info("Creating namespace: {}, with metadata: {}",
                Arrays.toString(namespace), metadata);
        if (namespace.length < 2) {
            throw toRuntime(new VastUserException(
                    format("Namespace identifier must include full schema path: %s",
                            Arrays.toString(namespace))));
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (!vastClient.schemaExists(tx, schemaName, endUser)) {
                Map<String, Object> newmap = metadata
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                Map.Entry::getValue));
                vastClient.createSchema(tx, schemaName,
                        new VastMetadataUtils().getPropertiesString(newmap),
                        endUser);
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
        throw new UnsupportedOperationException(
                "NDB catalog does not support altering namespaces");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException
    {
        final String endUser = getSessionUser(config);
        LOG.info("Dropping namespace: {}, cascade: {}",
                Arrays.toString(namespace), cascade);
        if (cascade) {
            throw new UnsupportedOperationException(
                    "NDB catalog does not support drop cascade");
        }
        if (namespace.length < 2) {
            throw toRuntime(new VastUserException(
                    format("Namespace identifier must include full schema path: %s",
                            Arrays.toString(namespace))));
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.schemaExists(tx, schemaName, endUser)) {
                vastClient.dropSchema(tx, schemaName, endUser);
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
        final String endUser = getSessionUser(config);
        LOG.debug("listTables {}", Arrays.toString(namespace));

        if (namespace.length < 2) {
            LOG.warn("Can't list tables without specifying schema");
            throw new NoSuchNamespaceException(namespace);
        }
        String schemaName = compose(namespace);
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (!vastClient.schemaExists(tx, schemaName, endUser)) {
                throw new NoSuchNamespaceException(namespace);
            }
            LOG.debug("Listing tables for schema name: {}", schemaName);
            try {
                return vastClient
                        .listTables(tx, schemaName, PAGE_SIZE, endUser)
                        .map(table -> Identifier.of(namespace, table.getName()))
                        .toArray(Identifier[]::new);
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
        final String endUser = getSessionUser(config);
        LOG.debug("tableExists {}", ident);

        String schemaName = compose(ident.namespace());
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            boolean exists = vastClient.tableExists(tx, schemaName,
                    ident.name(), endUser);
            LOG.debug("tableExists {} return {}", ident, exists);
            return exists;
        }
        catch (final RuntimeException re) {
            throw re;
        }
        catch (final Exception e) {
            throw new RuntimeException(
                    format("Table existence check failed during fetching table info for identifier %s",
                            ident.name()), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident)
            throws NoSuchTableException
    {
        LOG.debug("loadTable {}", ident);
        final String endUser = getSessionUser(config);

        if (ident.name().endsWith(NonAcidResolutionRule.VAST_ALLOW_NON_ACID)) {
            ident = Identifier.of(ident.namespace(),
                    ident.name().split(" ")[0]);
        }

        String tableName = ident.name();
        boolean failOnRCLS = false;
        if (tableName.endsWith(VAST_THROW_RCLS_ERROR)) {
            failOnRCLS = true;
            tableName = tableName.substring(0,
                    tableName.length() - VAST_THROW_RCLS_ERROR.length());
        }
        if (tableName.equals("partitions")) {
            return loadPartitionsTable(ident.namespace(), endUser);
        }
        String schemaName = compose(ident.namespace());
        boolean isImport = false;
        boolean isRowLevelOp = false;
        if (isImportDataTableName(tableName)) {
            isImport = true;
            tableName = getTableNameForAPI(ident.name());
            LOG.debug("loadTable importing into table {}", tableName);
        }
        else if (isForRowLevelOp(tableName)) {
            isRowLevelOp = true;
            tableName = trimTableNameFromRowLevelOpSuffix(tableName);
            LOG.debug("loadTable row level operation on table {}", tableName);
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            Optional<VastObjectDetails> vastTableHandleId = vastClient.getVastTableHandleId(
                    tx, schemaName, tableName, endUser);
            if (vastTableHandleId.isPresent()) {
                RowColumnSecurityResponse rowColumnSecurity = vastCatalogUtils.getRowColumnSecurity(
                        schemaName, tableName, endUser);
                if (rowColumnSecurity != null) {
                    if (rowColumnSecurity.getMaskedColumns() != null || !rowColumnSecurity
                            .getRowFilters()
                            .isEmpty()) {
                        if (failOnRCLS) {
                            throw new NoSuchTableException(ident);
                        }
                    }
                }
                TableLayout tableLayout = vastClient.fetchTableLayout(tx,
                        schemaName, tableName, PAGE_SIZE,
                        new QueryDataExtraParams(), endUser);
                List<Field> fields = tableLayout.getSchema().getFields();
                Map<String, String> additionalProperties = !tableLayout.hasSortedColumns() ?
                        Map.of() :
                        Map.of(SORTED_BY_PROPERTY, tableLayout
                                .getSortedColumns()
                                .stream()
                                .map(Field::getName)
                                .collect(Collectors.joining(",")));
                if (isImport) {
                    // Adjust schema of the table for only the fields the user mentioned as Spark is doing a strict validation
                    // Add at the end a field for the given imported filename
                    String fieldsList = ident.name().substring(ident
                            .name()
                            .indexOf(
                                    IMPORT_DATA_TABLE_NAME_SUFFIX) + IMPORT_DATA_TABLE_NAME_SUFFIX.length());
                    if (fieldsList.length() < 2 || fieldsList.charAt(
                            0) != '(' || fieldsList.charAt(
                            fieldsList.length() - 1) != ')') {
                        throw toRuntime(new VastUserException(
                                format("Illegal import data field list for table: %s (ident: %s)",
                                        tableName, ident.name())));
                    }
                    String[] splitFields = fieldsList.substring(1,
                            fieldsList.length() - 1).split(",");
                    Set<String> givenColumns = new HashSet<>(
                            splitFields.length);
                    for (String str : splitFields) {
                        String trimmed = str.trim();
                        if (!trimmed.isEmpty()) {
                            givenColumns.add(trimmed);
                        }
                    }
                    fields = fields.stream().filter(
                            f -> givenColumns.contains(f.getName())).collect(
                            Collectors.toList());
                    if (fields.size() != givenColumns.size()) {
                        throw toRuntime(new VastUserException(
                                format("Not all given columns exist in the table. ident: %s, fields.size(): %d, givenColumns.size(): %d",
                                        ident.name(), fields.size(),
                                        givenColumns.size())));
                    }
                    fields.add(IMPORT_DATA_HIDDEN_FIELD);
                }
                else if (isRowLevelOp) {
                    boolean dontNeedExpandedRowIds = !tableLayout.hasSortedColumns() && !tableLayout.hasPartitionColumns();
                    fields = Lists.asList(dontNeedExpandedRowIds ?
                                    VASTDB_SPARK_INT64_ROW_ID_NONNULL :
                                    VASTDB_SPARK_DEC128_ROW_ID_NONNULL,
                            fields.toArray(new Field[0]));
                }

                Schema partitionedSchema = !tableLayout.hasPartitionColumns() ?
                        new Schema(List.of()) :
                        vastClient.listColumns(tx, schemaName,
                                tableName + PartitionConstants.PIT_NAME_SUFFIX,
                                PAGE_SIZE, new QueryDataExtraParams(), endUser);
                StructType sparkSchema = TypeUtil.arrowFieldsListToSparkSchema(
                        fields);
                List<Field> partitionFields = partitionedSchema
                        .getFields()
                        .stream()
                        .filter(f -> !PartitionConstants.PIT_METADATA_COLUMN_NAMES.contains(
                                f.getName()))
                        .collect(Collectors.toList());
                StructType sparkPartitionedSchema = !tableLayout.hasPartitionColumns() ?
                        null :
                        TypeUtil.arrowFieldsListToSparkSchema(partitionFields);
                Transform[] partitioning = new Transform[tableLayout
                        .getPartitionColumnsMetadata()
                        .size()];
                Arrays.setAll(partitioning, index -> tryFromVastPartitionColumn(
                        tableLayout.getPartitionColumnsMetadata().get(index)));
                return makeVastTable(schemaName, tableName,
                        vastTableHandleId.get().getHandle(), sparkSchema, partitioning,
                        () -> this.vastClient, isImport, additionalProperties,
                        sparkPartitionedSchema);
            }
            else {
                throw new NoSuchTableException(ident);
            }
        }
        catch (final NoSuchTableException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (VastException ve) {
            throw new NoSuchTableException(
                    format("Could not load identifier %s", ident),
                    Option.apply(ve));
        }
        catch (final Exception e) {
            throw new RuntimeException(
                    format("Loading table failed during fetching table info for identifier %s",
                            ident.name()), e);
        }
    }

    private Table loadPartitionsTable(String[] name, String endUser)
            throws NoSuchTableException
    {
        LOG.debug("loadPartitionsTable for table {}", Arrays.toString(name));
        final String underlyingName = name[name.length - 1];
        final String tableName = underlyingName + PIT_NAME_SUFFIX;
        final String schemaName = compose(Arrays.copyOf(name, name.length - 1));
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            Optional<VastObjectDetails> vastTableHandleId = vastClient.getVastTableHandleId(
                    tx, schemaName, underlyingName, endUser);
            if (vastTableHandleId.isPresent()) {
                // TODO: with MS2 we also want row estimates
                List<Field> fields = vastClient
                        .listColumns(tx, schemaName, tableName, PAGE_SIZE,
                                new QueryDataExtraParams(), endUser)
                        .getFields()
                        .stream()
                        .filter(f -> !PartitionConstants.PIT_COLUMN_NAMES_TO_HIDE.contains(
                                f.getName()))
                        .collect(Collectors.toList());
                LOG.debug("loadPartitionsTable loaded PIT fields: {}", fields);
                StructType schema = TypeUtil.arrowFieldsListToSparkSchema(
                        fields);
                return makeVastPITTable(schemaName, tableName,
                        vastTableHandleId.get().getHandle(), new Transform[0],
                        () -> this.vastClient, Collections.emptyMap(), schema);
            }
            else {
                LOG.debug("loadPartitionsTable cannot get table handle for {}",
                        Arrays.toString(name));
                throw new NoSuchTableException(
                        Identifier.of(Arrays.copyOf(name, name.length - 1),
                                tableName));
            }
        }
        catch (final NoSuchTableException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception e) {
            throw new RuntimeException(
                    format("Loading table failed during fetching table info for identifier %s",
                            tableName), e);
        }
    }

    @Override
    public Table createTable(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException
    {
        final String endUser = getSessionUser(config);
        LOG.info(
                "Creating table identifier: {}, schema: {}, partitions: {}, properties: {}",
                ident, schema, Arrays.toString(partitions), properties);

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();
        if (isImportDataTableName(tableName)) {
            throw toRuntime(new VastUserException(
                    format("Illegal table name for create table: %s",
                            tableName)));
        }

        if (partitions.length > 4) {
            throw toRuntime(new VastUserException(
                    "Too many partitioning columns are provided.  Maximum is 4"));
        }

        Map<String, Object> convertedProperties = convertProperties(properties);
        List<String> colNames = Arrays.asList(schema.fieldNames());
        Map<String, String> partitionDefs = new HashMap<>(partitions.length);
        for (int i = 0; i < partitions.length; i++) {
            LOG.debug("transform: {}", partitions[i]);
            NamedReference colRef = (NamedReference) partitions[i].children()[partitions[i].children().length - 1];
            int colIdx = colNames.indexOf(colRef.toString());
            if (colIdx < 0) {
                throw toRuntime(new VastUserException(
                        format("%s: Unknown column name",
                                partitions[i].toString())));
            }
            String transformName = partitions[i].name();
            Integer arg = null;
            if (transformName.endsWith("s")) {
                transformName = transformName.substring(0,
                        transformName.length() - 1);
            }
            else if (transformName.startsWith("truncate")) {
                arg = Integer.parseInt(
                        transformName.substring("truncate_".length()));
                transformName = "truncate";
            }
            else if (partitions[i].children().length > 1) {
                arg = (Integer) ((Literal) partitions[i].children()[0]).value();
            }
            TransformSerializer ts = new TransformSerializer(transformName,
                    colIdx, arg);

            String key = format(TABULAR_PARTITION_KEY_TEMPLATE, i);
            try {
                String json = mapper.writeValueAsString(ts);
                LOG.debug("json: {}", json);
                partitionDefs.put(key, json);
            }
            catch (JsonProcessingException e) {
                throw toRuntime(e);
            }
        }
        List<Field> fieldList = TypeUtil.adaptVerifiedSparkSchemaToArrowFieldsList(
                schema);
        CreateTableContext ctx;
        try {
            ctx = CreateTableContext.create(schemaName, tableName, fieldList,
                    Optional.empty(), convertedProperties, partitionDefs, true);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.schemaExists(tx, schemaName, endUser)) {
                if (!vastClient.tableExists(tx, schemaName, tableName,
                        endUser)) {
                    vastClient.createTable(tx, ctx, endUser);
                    String vastTableHandleId = vastClient.getVastTableHandleId(
                            tx, schemaName, tableName, endUser).orElseThrow(
                            () -> VastExceptionFactory.tableHandleIdNotFound(
                                    schemaName, tableName)).getHandle();
                    tx.setCommit(true);
                    return makeVastTable(schemaName, tableName,
                            vastTableHandleId, schema, new Transform[0],
                            () -> vastClient, false, new HashMap<>(), null);
                }
                else {
                    throw new TableAlreadyExistsException(ident);
                }
            }
            else {
                throw new NoSuchNamespaceException(ident.namespace());
            }
        }
        catch (final TableAlreadyExistsException | NoSuchNamespaceException |
                RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(
                    format("Creating table failed during putting table info to server for identifier %s",
                            ident.name()), any);
        }
    }

    private Map<String, Object> convertProperties(
            Map<String, String> properties)
    {
        Map<String, Object> convertedProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Object value = entry.getValue();
            if (SORTED_BY_PROPERTY.equals(entry.getKey())) {
                value = getSortedByColumns(entry.getValue());
            }
            convertedProperties.put(entry.getKey(), value);
        }
        return convertedProperties;
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes)
            throws NoSuchTableException
    {
        final String endUser = getSessionUser(config);
        LOG.debug("alterTable {}, {}", ident, Arrays.toString(changes));

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();

        if (isImportDataTableName(tableName)) {
            throw toRuntime(new VastUserException(
                    format("Illegal table name for alter table: %s",
                            tableName)));
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            VastTableChangeFactory vastTableChangeFactory = new VastTableChangeFactory(
                    config, schemaName, tableName);
            Optional<VastObjectDetails> vastTableHandleId = vastClient.getVastTableHandleId(
                    tx, schemaName, tableName, endUser);
            if (vastTableHandleId.isPresent()) {
                return tx.executeWithRollbackOnFailure(() -> {
                    VastTableChange vastTableChange = vastTableChangeFactory.compose(
                            changes);
                    vastTableChange.accept(vastClient, tx);
                    tx.setCommit(true);
                    TableLayout layout = vastClient.fetchTableLayout(tx,
                            schemaName, tableName, PAGE_SIZE,
                            new QueryDataExtraParams(), endUser);
                    List<Field> fields = layout.getSchema().getFields();
                    Map<String, String> additionalProperties = !layout.hasSortedColumns() ?
                            Map.of() :
                            Map.of(SORTED_BY_PROPERTY, layout
                                    .getSortedColumns()
                                    .stream()
                                    .map(Field::getName)
                                    .collect(Collectors.joining(",")));
                    // TODO: elysium support (partitioning)
                    return makeVastTable(schemaName, tableName,
                            vastTableHandleId.get().getHandle(),
                            TypeUtil.arrowFieldsListToSparkSchema(fields),
                            new Transform[0], () -> vastClient, false,
                            additionalProperties, null);
                });
            }
            else {
                throw new NoSuchTableException(ident);
            }
        }
        catch (final NoSuchTableException | RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(
                    format("Failed applying table changes for identifier %s",
                            ident.name()), any);
        }
    }

    @Override
    public boolean dropTable(Identifier ident)
    {
        final String endUser = getSessionUser(config);
        LOG.debug("dropTable {}", ident);

        String schemaName = compose(ident.namespace());
        String tableName = ident.name();

        DropTableContext ctx = new DropTableContext(schemaName, tableName);

        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.tableExists(tx, schemaName, tableName, endUser)) {
                vastClient.dropTable(tx, ctx, endUser);
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
            throw new RuntimeException(
                    format("Failed dropping table for identifier %s",
                            ident.name()), e);
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException
    {
        final String endUser = getSessionUser(config);
        LOG.info("Renaming table {} to {}", oldIdent, newIdent);
        if (newIdent.namespace().length < 2) {
            throw new RuntimeException(
                    format("Failed renaming table - new name must include valid table path: %s",
                            newIdent));
        }
        String oldBucket = oldIdent.namespace()[0];
        String newBucket = newIdent.namespace()[0];
        if (!oldBucket.equalsIgnoreCase(newBucket)) {
            throw new RuntimeException(
                    format("Failed renaming table - changing bucket is not supported: %s, %s",
                            oldIdent, newIdent));
        }
        String schemaName = compose(oldIdent.namespace());
        String tableName = oldIdent.name();
        String newFullSchemaPath = compose(newIdent.namespace());
        String newSchemaName = compose(
                Arrays.copyOfRange(newIdent.namespace(), 1,
                        newIdent.namespace().length));
        String newTableName = newIdent.name();

        if (isImportDataTableName(newTableName)) {
            throw toRuntime(new VastUserException(
                    format("Illegal table name for rename table: %s",
                            newTableName)));
        }
        String format = format("%s/%s", newSchemaName, newTableName);
        AlterTableContext ctx;
        try {
            ctx = AlterTableContext.create(format, null, null, true);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (!vastClient.tableExists(tx, schemaName, tableName, endUser)) {
                throw new NoSuchTableException(oldIdent);
            }
            if (vastClient.tableExists(tx, newFullSchemaPath, newTableName,
                    endUser)) {
                throw new TableAlreadyExistsException(newIdent);
            }
            vastClient.alterTable(tx, schemaName, tableName, ctx, endUser);
            tx.setCommit(true);
        }
        catch (final NoSuchTableException | TableAlreadyExistsException |
                RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception e) {
            throw new RuntimeException(
                    format("Renaming table failed during update of table info for identifier %s",
                            oldIdent), e);
        }
    }

    private VastTable makeVastTable(String schemaName, String tableName,
            String handleID, StructType schema, Transform[] partitioning,
            Supplier<VastClient> clientSupplier, boolean forImportData,
            Map<String, String> additionalProperties,
            StructType partitionSchema)
    {
        if (partitionSchema != null) {
            LOG.debug("Creating VastPartitionedTable for table {}", tableName);
            return new VastPartitionedTable(vastCatalogUtils, schemaName,
                    tableName, handleID, schema, partitioning, clientSupplier,
                    forImportData, sparkConfValidator.writeError,
                    additionalProperties, partitionSchema);
        }
        return new VastTable(vastCatalogUtils, schemaName, tableName, handleID,
                schema, partitioning, clientSupplier, forImportData,
                sparkConfValidator.writeError, additionalProperties);
    }

    private VastTable makeVastPITTable(String schemaName, String tableName,
            String handleID, Transform[] partitioning,
            Supplier<VastClient> clientSupplier,
            Map<String, String> additionalProperties,
            StructType partitionSchema)
    {
        LOG.debug("Creating VastPITTable for table {}", tableName);
        return new VastPITTable(vastCatalogUtils, schemaName, tableName,
                handleID, partitionSchema, partitioning, clientSupplier,
                sparkConfValidator.writeError, additionalProperties);
    }

    public Identifier[] listViews(String... namespace)
            throws NoSuchNamespaceException
    {
        final String endUser = getSessionUser(config);
        LOG.debug("listViews {}", Arrays.toString(namespace));

        if (namespace.length < 2) {
            LOG.warn("Can't list views without specifying schema");
            throw new NoSuchNamespaceException(namespace);
        }

        final String schemaName = compose(namespace);
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (!vastClient.schemaExists(tx, schemaName, endUser)) {
                throw new NoSuchNamespaceException(namespace);
            }
            LOG.debug("Listing views for schema name: {}", schemaName);
            try {
                return vastClient
                        .listViews(tx, schemaName, PAGE_SIZE, endUser)
                        .map(viewName -> Identifier.of(namespace, viewName))
                        .toArray(Identifier[]::new);
            }
            catch (final VastServerException | VastUserException e) {
                throw toRuntime(e);
            }
        }
        catch (final VastException e) {
            throw toRuntime(e);
        }
    }

    public VastView loadView(final Identifier ident,
            Optional<VastTransaction> existingTransaction)
            throws NoSuchViewException
    {
        final String endUser = getSessionUser(config);
        LOG.debug("loadViewSql {}", ident);
        String[] namespace = ident.namespace();
        final String schemaName = compose(namespace);
        final String viewName = ident.name();
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrapVastTransactionOrCreateNew(
                existingTransaction, transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.schemaExists(tx, schemaName, endUser)) {
                if (vastClient.viewExists(tx, schemaName, viewName, endUser)) {
                    SimpleVastTransaction transaction = new SimpleVastTransaction(
                            tx.getId());
                    List<Field> fields = vastClient
                            .listColumns(tx, schemaName, viewName, 1000,
                                    new QueryDataExtraParams(), endUser)
                            .getFields();
                    return sparkViewsMetadataReaderFactory
                            .instance()
                            .getVastView(transaction, schemaName, viewName,
                                    namespace, fields, null, endUser);
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
            throw new RuntimeException(
                    format("Loading view failed for identifier %s", viewName),
                    any);
        }
    }

    public void createView(SparkViewMetadata ctx, boolean replace,
            Optional<VastTransaction> existingTransaction)
            throws ViewAlreadyExistsException, NoSuchNamespaceException
    {
        final String endUser = getSessionUser(config);
        LOG.debug("createView: CreateSparkViewContext: {}", ctx);
        final String schemaName = compose(ctx.getIdentifier().namespace());
        final String viewName = ctx.getIdentifier().name();
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrapVastTransactionOrCreateNew(
                existingTransaction, transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.schemaExists(tx, schemaName, endUser)) {
                if (!vastClient.viewExists(tx, schemaName, viewName, endUser)) {
                    vastClient.createView(tx, ctx.toVastCreateViewContext(),
                            endUser);
                }
                else {
                    if (replace) {
                        LOG.debug("createView: replacing existing view");
                        vastClient.dropView(tx,
                                new DropViewContext(schemaName, viewName),
                                endUser);
                        vastClient.createView(tx, ctx.toVastCreateViewContext(),
                                endUser);
                    }
                    else {
                        throw new ViewAlreadyExistsException(
                                ctx.getIdentifier());
                    }
                }
            }
            else {
                throw new NoSuchNamespaceException(
                        ctx.getIdentifier().namespace());
            }
        }
        catch (final ViewAlreadyExistsException | NoSuchNamespaceException |
                RuntimeException rethrowable) {
            throw rethrowable;
        }
        catch (final Exception any) {
            throw new RuntimeException(
                    format("Creating view failed during putting view info to server for identifier %s",
                            viewName), any);
        }
    }

    public boolean dropView(Identifier ident,
            Optional<VastTransaction> existingTransaction)
    {
        final String endUser = getSessionUser(config);
        LOG.debug("dropView {}", ident);

        final String schemaName = compose(ident.namespace());
        final String viewName = ident.name();

        final DropViewContext ctx = new DropViewContext(schemaName, viewName);

        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrapVastTransactionOrCreateNew(
                existingTransaction, transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            if (vastClient.viewExists(tx, schemaName, viewName, endUser)) {
                vastClient.dropView(tx, ctx, endUser);
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
            throw new RuntimeException(
                    format("Failed dropping view for identifier %s",
                            ident.name()), e);
        }
    }

    public VastCatalogUtils getVastCatalogUtils()
    {
        return vastCatalogUtils;
    }

    @VisibleForTesting
    protected void setVastCatalogUtils(VastCatalogUtils vastCatalogUtils)
    {
        this.vastCatalogUtils = vastCatalogUtils;
    }

    protected void setSparkViewsMetadataReaderFactory(
            SparkViewsMetadataReaderFactory sparkViewsMetadataReaderFactory)
    {
        this.sparkViewsMetadataReaderFactory = sparkViewsMetadataReaderFactory;
    }

    private class TransformSerializer
    {
        @JsonProperty("transform") final String transform;
        @JsonProperty("column-index") final int columnIndex;
        @JsonProperty("transform-arg") final Integer transformArg;

        public TransformSerializer(String t, int idx, Integer a)
        {
            this.transform = t;
            this.columnIndex = idx;
            this.transformArg = a;
        }
    }
}
