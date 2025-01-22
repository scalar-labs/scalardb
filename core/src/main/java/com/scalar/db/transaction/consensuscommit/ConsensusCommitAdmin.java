package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.buildTransactionTableMetadata;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getBeforeImageColumnName;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getNonPrimaryKeyColumns;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.removeTransactionMetaColumns;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitAdmin implements DistributedTransactionAdmin {

  private final ConsensusCommitConfig config;
  private final DistributedStorageAdmin admin;
  private final String coordinatorNamespace;
  private final boolean isIncludeMetadataEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitAdmin(DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  public ConsensusCommitAdmin(DatabaseConfig databaseConfig) {
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    admin = storageFactory.getStorageAdmin();

    config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  @VisibleForTesting
  ConsensusCommitAdmin(
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      boolean isIncludeMetadataEnabled) {
    this.config = config;
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
    admin.createTable(
        coordinatorNamespace, Coordinator.TABLE, getCoordinatorTableMetadata(), options);
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

    admin.createTable(namespace, table, buildTransactionTableMetadata(metadata), options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    checkNamespace(namespace);

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
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.repairTable(namespace, table, buildTransactionTableMetadata(metadata), options);
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) throws ExecutionException {
    admin.repairTable(
        coordinatorNamespace, Coordinator.TABLE, getCoordinatorTableMetadata(), options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    checkNamespace(namespace);

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
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    checkNamespace(namespace);

    TableMetadata tableMetadata = admin.getTableMetadata(namespace, table);
    if (tableMetadata != null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)));
    }
    tableMetadata = admin.getImportTableMetadata(namespace, table, overrideColumnsType);

    // add transaction metadata columns
    for (Map.Entry<String, DataType> entry :
        ConsensusCommitUtils.getTransactionMetaColumns().entrySet()) {
      admin.addRawColumnToTable(namespace, table, entry.getKey(), entry.getValue());
    }

    // add before image columns
    Set<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(tableMetadata);
    for (String columnName : nonPrimaryKeyColumns) {
      String beforeColumnName = getBeforeImageColumnName(columnName, tableMetadata);
      DataType columnType = tableMetadata.getColumnDataType(columnName);
      admin.addRawColumnToTable(namespace, table, beforeColumnName, columnType);
    }

    // add ScalarDB metadata
    admin.repairTable(namespace, table, buildTransactionTableMetadata(tableMetadata), options);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return admin.getNamespaceNames().stream()
        .filter(namespace -> !namespace.equals(coordinatorNamespace))
        .collect(Collectors.toSet());
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

  private TableMetadata getCoordinatorTableMetadata() {
    if (config.isCoordinatorGroupCommitEnabled()) {
      return Coordinator.TABLE_METADATA_WITH_GROUP_COMMIT_ENABLED;
    } else {
      return Coordinator.TABLE_METADATA_WITH_GROUP_COMMIT_DISABLED;
    }
  }
}
