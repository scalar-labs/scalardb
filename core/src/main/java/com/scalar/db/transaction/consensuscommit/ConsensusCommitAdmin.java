package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.buildTransactionTableMetadata;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getBeforeImageColumnName;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getNonPrimaryKeyColumns;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.removeTransactionMetaColumns;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitAdmin implements DistributedTransactionAdmin {

  public static final String TRANSACTION_METADATA_DECOUPLING = "transaction_metadata_decoupling";

  private static final String TRANSACTION_METADATA_DECOUPLING_DATA_TABLE_SUFFIX = "_data";
  private static final String TRANSACTION_METADATA_DECOUPLING_METADATA_TABLE_SUFFIX =
      "_tx_metadata";
  private static final String TRANSACTION_METADATA_DECOUPLING_IMPORTED_TABLE_SUFFIX = "_scalardb";

  private final DistributedStorageAdmin admin;
  private final String coordinatorNamespace;
  private final boolean isIncludeMetadataEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitAdmin(DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    this.admin = admin;
    ConsensusCommitConfig config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  public ConsensusCommitAdmin(DatabaseConfig databaseConfig) {
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    admin = storageFactory.getStorageAdmin();

    ConsensusCommitConfig config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  @VisibleForTesting
  ConsensusCommitAdmin(
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      boolean isIncludeMetadataEnabled) {
    this.admin = admin;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
  }

  @Override
  public void createCoordinatorTables(Map<String, String> options) throws ExecutionException {
    if (coordinatorTablesExist()) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_COORDINATOR_TABLES_ALREADY_EXIST.buildMessage());
    }

    admin.createNamespace(coordinatorNamespace, options);
    admin.createTable(coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, options);
  }

  @Override
  public void dropCoordinatorTables() throws ExecutionException {
    if (!coordinatorTablesExist()) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_COORDINATOR_TABLES_NOT_FOUND.buildMessage());
    }

    admin.dropTable(coordinatorNamespace, Coordinator.TABLE);
    admin.dropNamespace(coordinatorNamespace);
  }

  @Override
  public void truncateCoordinatorTables() throws ExecutionException {
    if (!coordinatorTablesExist()) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_COORDINATOR_TABLES_NOT_FOUND.buildMessage());
    }

    admin.truncateTable(coordinatorNamespace, Coordinator.TABLE);
  }

  @Override
  public boolean coordinatorTablesExist() throws ExecutionException {
    return admin.tableExists(coordinatorNamespace, Coordinator.TABLE);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);

    if (isTransactionMetadataDecouplingEnabled(options)) {
      // For transaction metadata decoupling mode

      throwIfTransactionMetadataDecouplingNotSupportedStorage(namespace);

      String dataTableName = table + TRANSACTION_METADATA_DECOUPLING_DATA_TABLE_SUFFIX;
      String txMetadataTableName = table + TRANSACTION_METADATA_DECOUPLING_METADATA_TABLE_SUFFIX;

      if (admin.tableExists(namespace, dataTableName)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_ALREADY_EXISTS.buildMessage(
                ScalarDbUtils.getFullTableName(namespace, dataTableName)));
      }
      if (admin.tableExists(namespace, txMetadataTableName)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_ALREADY_EXISTS.buildMessage(
                ScalarDbUtils.getFullTableName(namespace, txMetadataTableName)));
      }

      // Create a data table
      admin.createTable(namespace, dataTableName, metadata, options);

      // Create a transaction metadata table
      admin.createTable(
          namespace,
          txMetadataTableName,
          ConsensusCommitUtils.buildTransactionMetadataTableMetadata(metadata),
          options);

      // Create a virtual table based on the data table and the transaction metadata table
      admin.createVirtualTable(
          namespace,
          table,
          namespace,
          dataTableName,
          namespace,
          txMetadataTableName,
          VirtualTableJoinType.INNER,
          options);
      return;
    }

    admin.createTable(namespace, table, buildTransactionTableMetadata(metadata), options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    checkNamespace(namespace);

    if (admin.getVirtualTableInfo(namespace, table).isPresent()) {
      // For transaction metadata decoupling tables

      // Drop a virtual table
      admin.dropTable(namespace, table);

      if (table.endsWith(TRANSACTION_METADATA_DECOUPLING_IMPORTED_TABLE_SUFFIX)) {
        // For imported tables

        String originalTableName =
            table.substring(
                0, table.length() - TRANSACTION_METADATA_DECOUPLING_IMPORTED_TABLE_SUFFIX.length());

        // Drop the original data table and the transaction metadata table
        admin.dropTable(namespace, originalTableName);
        admin.dropTable(
            namespace, originalTableName + TRANSACTION_METADATA_DECOUPLING_METADATA_TABLE_SUFFIX);
        return;
      } else {
        // For created tables

        // Drop the data table and the transaction metadata table
        admin.dropTable(namespace, table + TRANSACTION_METADATA_DECOUPLING_DATA_TABLE_SUFFIX);
        admin.dropTable(namespace, table + TRANSACTION_METADATA_DECOUPLING_METADATA_TABLE_SUFFIX);
      }
      return;
    }

    // For normal tables
    admin.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    checkNamespace(namespace);

    admin.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    checkNamespace(namespace);

    admin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.dropIndex(namespace, table, columnName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    if (namespace.equals(coordinatorNamespace)) {
      return null;
    }

    TableMetadata metadata = admin.getTableMetadata(namespace, table);
    if (metadata == null) {
      return null;
    }
    if (!ConsensusCommitUtils.isTransactionTableMetadata(metadata)) {
      return null;
    }
    if (isIncludeMetadataEnabled) {
      return metadata;
    }
    return removeTransactionMetaColumns(metadata);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    if (namespace.equals(coordinatorNamespace)) {
      return Collections.emptySet();
    }

    Set<String> tables = new HashSet<>();
    for (String table : admin.getNamespaceTableNames(namespace)) {
      // Remove non transaction table
      if (getTableMetadata(namespace, table) != null) {
        tables.add(table);
      }
    }

    return tables;
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (namespace.equals(coordinatorNamespace)) {
      return false;
    }

    return admin.namespaceExists(namespace);
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.repairNamespace(namespace, options);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, table, "repairTable()");

    admin.repairTable(namespace, table, buildTransactionTableMetadata(metadata), options);
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) throws ExecutionException {
    admin.repairNamespace(coordinatorNamespace, options);
    admin.repairTable(coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, table, "addNewColumnToTable()");

    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }
    String beforeColumnName = getBeforeImageColumnName(columnName, tableMetadata);

    admin.addNewColumnToTable(namespace, table, columnName, columnType);
    admin.addNewColumnToTable(namespace, table, beforeColumnName, columnType);
  }

  @Override
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, table, "dropColumnFromTable()");

    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }
    String beforeColumnName = getBeforeImageColumnName(columnName, tableMetadata);

    admin.dropColumnFromTable(namespace, table, columnName);
    admin.dropColumnFromTable(namespace, table, beforeColumnName);
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, table, "renameColumn()");

    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }
    if (tableMetadata.getPartitionKeyNames().contains(oldColumnName)
        || tableMetadata.getClusteringKeyNames().contains(oldColumnName)) {
      admin.renameColumn(namespace, table, oldColumnName, newColumnName);
    } else {
      String oldBeforeColumnName = getBeforeImageColumnName(oldColumnName, tableMetadata);
      String newBeforeColumnName = getBeforeImageColumnName(newColumnName, tableMetadata);

      admin.renameColumn(namespace, table, oldColumnName, newColumnName);
      admin.renameColumn(namespace, table, oldBeforeColumnName, newBeforeColumnName);
    }
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, table, "alterColumnType()");

    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }
    if (tableMetadata.getPartitionKeyNames().contains(columnName)
        || tableMetadata.getClusteringKeyNames().contains(columnName)) {
      admin.alterColumnType(namespace, table, columnName, newColumnType);
    } else {
      String beforeColumnName = getBeforeImageColumnName(columnName, tableMetadata);

      admin.alterColumnType(namespace, table, columnName, newColumnType);
      admin.alterColumnType(namespace, table, beforeColumnName, newColumnType);
    }
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    checkNamespace(namespace);
    throwIfTransactionMetadataDecouplingTable(namespace, oldTableName, "renameTable()");

    admin.renameTable(namespace, oldTableName, newTableName);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return admin.getNamespaceNames().stream()
        .filter(namespace -> !namespace.equals(coordinatorNamespace))
        .collect(Collectors.toSet());
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    checkNamespace(namespace);

    if (isTransactionMetadataDecouplingEnabled(options)) {
      // For transaction metadata decoupling mode

      throwIfTransactionMetadataDecouplingNotSupportedStorage(namespace);

      String importedTableName = table + TRANSACTION_METADATA_DECOUPLING_IMPORTED_TABLE_SUFFIX;
      String txMetadataTableName = table + TRANSACTION_METADATA_DECOUPLING_METADATA_TABLE_SUFFIX;

      if (admin.tableExists(namespace, txMetadataTableName)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_ALREADY_EXISTS.buildMessage(
                ScalarDbUtils.getFullTableName(namespace, txMetadataTableName)));
      }
      if (admin.tableExists(namespace, importedTableName)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_ALREADY_EXISTS.buildMessage(
                ScalarDbUtils.getFullTableName(namespace, importedTableName)));
      }

      // import the original table as a data table
      admin.importTable(namespace, table, options, overrideColumnsType);

      TableMetadata dataTableMetadata = admin.getTableMetadata(namespace, table);
      assert dataTableMetadata != null;

      // create a transaction metadata table
      admin.createTable(
          namespace,
          txMetadataTableName,
          ConsensusCommitUtils.buildTransactionMetadataTableMetadata(dataTableMetadata),
          options);

      // create a virtual table based on the data table and the transaction metadata table
      admin.createVirtualTable(
          namespace,
          importedTableName,
          namespace,
          table,
          namespace,
          txMetadataTableName,
          VirtualTableJoinType.LEFT_OUTER,
          options);
      return;
    }

    // import the original table
    admin.importTable(namespace, table, options, overrideColumnsType);

    TableMetadata tableMetadata = admin.getTableMetadata(namespace, table);
    assert tableMetadata != null;

    // add transaction metadata columns
    for (Map.Entry<String, DataType> entry :
        ConsensusCommitUtils.getTransactionMetaColumns().entrySet()) {
      admin.addNewColumnToTable(namespace, table, entry.getKey(), entry.getValue());
    }

    // add before image columns
    Set<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(tableMetadata);
    for (String columnName : nonPrimaryKeyColumns) {
      String beforeColumnName = getBeforeImageColumnName(columnName, tableMetadata);
      DataType columnType = tableMetadata.getColumnDataType(columnName);
      admin.addNewColumnToTable(namespace, table, beforeColumnName, columnType);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    admin.upgrade(options);

    upgradeCoordinatorTable();
  }

  private void upgradeCoordinatorTable() throws ExecutionException {
    TableMetadata currentMetadata = admin.getTableMetadata(coordinatorNamespace, Coordinator.TABLE);
    if (currentMetadata == null) {
      return;
    }
    // These columns were recently added. Therefore, it's possible Coordinator tables created
    // earlier don't have these columns.
    List<String> potentialMissingColumnNames = ImmutableList.of(Attribute.CHILD_IDS);

    // Verify the potentially missing columns.
    for (String columnName : potentialMissingColumnNames) {
      if (currentMetadata.getColumnNames().contains(columnName)) {
        continue;
      }
      // The `upgrade` command doesn't migrate key columns.
      if (Coordinator.TABLE_METADATA.getPartitionKeyNames().contains(columnName)
          || Coordinator.TABLE_METADATA.getClusteringKeyNames().contains(columnName)
          || Coordinator.TABLE_METADATA.getSecondaryIndexNames().contains(columnName)) {
        // In practice, this currently doesn't happen. Special handling would be needed if we add
        // a key column in the Coordinator table metadata in the future.
        throw new IllegalStateException(
            String.format(
                "Failed to upgrade the Coordinator table schema. Key columns can't be migrated. Column: %s",
                columnName));
      }
    }

    for (String columnName : potentialMissingColumnNames) {
      if (currentMetadata.getColumnNames().contains(columnName)) {
        continue;
      }
      DataType columnDataType = Coordinator.TABLE_METADATA.getColumnDataType(columnName);
      admin.addNewColumnToTable(
          coordinatorNamespace, Coordinator.TABLE, columnName, columnDataType);
    }
  }

  @Override
  public void close() {
    admin.close();
  }

  private void checkNamespace(String namespace) {
    if (namespace.equals(coordinatorNamespace)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_COORDINATOR_NAMESPACE_SPECIFIED.buildMessage(namespace));
    }
  }

  private boolean isTransactionMetadataDecouplingEnabled(Map<String, String> options) {
    return options.containsKey(TRANSACTION_METADATA_DECOUPLING)
        && options.get(TRANSACTION_METADATA_DECOUPLING).equalsIgnoreCase("true");
  }

  private void throwIfTransactionMetadataDecouplingNotSupportedStorage(String namespace)
      throws ExecutionException {
    StorageInfo storageInfo = admin.getStorageInfo(namespace);
    if ((storageInfo.getMutationAtomicityUnit() != StorageInfo.MutationAtomicityUnit.STORAGE
        && storageInfo.getMutationAtomicityUnit() != StorageInfo.MutationAtomicityUnit.NAMESPACE)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_TRANSACTION_METADATA_DECOUPLING_NOT_SUPPORTED_STORAGE
              .buildMessage(storageInfo.getStorageName()));
    }
    if (!storageInfo.isConsistentVirtualTableReadGuaranteed()) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_TRANSACTION_METADATA_CONSISTENT_READS_NOT_GUARANTEED_STORAGE
              .buildMessage(storageInfo.getStorageName()));
    }
  }

  private void throwIfTransactionMetadataDecouplingTable(
      String namespace, String table, String method) throws ExecutionException {
    if (admin.tableExists(namespace, table)
        && admin.getVirtualTableInfo(namespace, table).isPresent()) {
      throw new UnsupportedOperationException(
          "Currently, " + method + " is not supported for transaction metadata decoupling tables");
    }
  }
}
