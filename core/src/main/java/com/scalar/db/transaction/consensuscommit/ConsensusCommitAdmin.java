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
import com.scalar.db.common.CoreError;
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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitAdmin implements DistributedTransactionAdmin {

  private final ConsensusCommitConfig config;
  private final DistributedStorageAdmin admin;
  private final String coordinatorNamespace;
  private final boolean isIncludeMetadataEnabled;
  private final boolean isIndexEventuallyConsistentReadEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitAdmin(DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    isIndexEventuallyConsistentReadEnabled = config.isIndexEventuallyConsistentReadEnabled();
  }

  public ConsensusCommitAdmin(DatabaseConfig databaseConfig) {
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    admin = storageFactory.getStorageAdmin();

    config = new ConsensusCommitConfig(databaseConfig);
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    isIndexEventuallyConsistentReadEnabled = config.isIndexEventuallyConsistentReadEnabled();
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
    isIndexEventuallyConsistentReadEnabled = config.isIndexEventuallyConsistentReadEnabled();
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

    admin.createTable(
        namespace,
        table,
        buildTransactionTableMetadata(metadata, isIndexEventuallyConsistentReadEnabled),
        options);
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

    if (!isIndexEventuallyConsistentReadEnabled) {
      // Also create an index on the before image column if it exists
      TableMetadata rawMetadata = admin.getTableMetadata(namespace, table);
      if (rawMetadata != null) {
        String beforeColumnName = Attribute.BEFORE_PREFIX + columnName;
        if (rawMetadata.getColumnNames().contains(beforeColumnName)) {
          admin.createIndex(namespace, table, beforeColumnName, options);
        }
      }
    }
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    checkNamespace(namespace);

    admin.dropIndex(namespace, table, columnName);

    if (!isIndexEventuallyConsistentReadEnabled) {
      // Also drop the index on the before image column if it exists
      TableMetadata rawMetadata = admin.getTableMetadata(namespace, table);
      if (rawMetadata != null) {
        String beforeColumnName = Attribute.BEFORE_PREFIX + columnName;
        if (rawMetadata.getColumnNames().contains(beforeColumnName)) {
          admin.dropIndex(namespace, table, beforeColumnName, true);
        }
      }
    }
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

    admin.repairTable(
        namespace,
        table,
        buildTransactionTableMetadata(metadata, isIndexEventuallyConsistentReadEnabled),
        options);
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) throws ExecutionException {
    admin.createNamespace(coordinatorNamespace, true, options);

    // Snapshot the current Coordinator table metadata before repairTable upserts the desired one.
    TableMetadata currentMetadata = admin.getTableMetadata(coordinatorNamespace, Coordinator.TABLE);

    // Pick the desired schema:
    // - If the existing Coordinator already has the CHILD_IDS column (i.e., it was previously
    //   created with group commit enabled), preserve the WITH_GROUP_COMMIT_ENABLED schema
    //   regardless of the current config value. The ScalarDB-side metadata must reflect the
    //   physical column set; the runtime config independently decides whether to USE the column.
    //   Without this, toggling group commit from true to false and then running repair would
    //   upsert no-child_ids metadata against a with-child_ids physical table, leaving them out
    //   of sync (and breaking subsequent toggles back to enabled, where ALTER TABLE ADD COLUMN
    //   would fail because the column already exists physically).
    // - Otherwise (no existing Coordinator, or existing Coordinator without CHILD_IDS), use the
    //   config-dependent schema, which lets a user upgrade a pre-group-commit Coordinator to
    //   WITH_GROUP_COMMIT_ENABLED by toggling the config and calling repairCoordinatorTables.
    TableMetadata coordinatorTableMetadata;
    if (currentMetadata != null && currentMetadata.getColumnNames().contains(Attribute.CHILD_IDS)) {
      coordinatorTableMetadata = Coordinator.TABLE_METADATA_WITH_GROUP_COMMIT_ENABLED;
    } else {
      coordinatorTableMetadata = getCoordinatorTableMetadata();
    }

    // Upgrade the schema (ALTER TABLE ADD COLUMN for any non-key columns the desired schema
    // requires that the existing Coordinator is missing) BEFORE repairTable. addNewColumnToTable
    // checks the existing metadata via getTableMetadata, so it must run while the metadata still
    // reflects the pre-upgrade column set. If repairTable runs first, it upserts the metadata to
    // the desired schema (which already includes the column), and the subsequent
    // addNewColumnToTable call would be rejected with "column already exists" against the
    // ScalarDB-side metadata even though the physical column does not yet exist.
    upgradeCoordinatorTableSchema(currentMetadata, coordinatorTableMetadata);

    admin.repairTable(coordinatorNamespace, Coordinator.TABLE, coordinatorTableMetadata, options);
  }

  // Adds any non-key columns the desired Coordinator table schema requires that the existing
  // Coordinator table is still missing. Specifically handles the case where group commit is being
  // turned on against a Coordinator table that was previously created with group commit disabled
  // (i.e., without the {@code child_ids} column). Mirrors the column-migration logic that master
  // implements in upgradeCoordinatorTable() under the (master-only) {@code upgrade()} API.
  private void upgradeCoordinatorTableSchema(
      @Nullable TableMetadata currentMetadata, TableMetadata coordinatorTableMetadata)
      throws ExecutionException {
    if (currentMetadata == null) {
      return;
    }
    for (String columnName : coordinatorTableMetadata.getColumnNames()) {
      if (currentMetadata.getColumnNames().contains(columnName)) {
        continue;
      }
      if (coordinatorTableMetadata.getPartitionKeyNames().contains(columnName)
          || coordinatorTableMetadata.getClusteringKeyNames().contains(columnName)
          || coordinatorTableMetadata.getSecondaryIndexNames().contains(columnName)) {
        // In practice, this currently doesn't happen. Special handling would be needed if a key
        // column were ever added to the Coordinator table metadata in the future.
        throw new IllegalStateException(
            String.format(
                "Failed to upgrade the Coordinator table schema. Key columns can't be migrated. Column: %s",
                columnName));
      }
    }

    for (String columnName : coordinatorTableMetadata.getColumnNames()) {
      if (currentMetadata.getColumnNames().contains(columnName)) {
        continue;
      }
      DataType columnDataType = coordinatorTableMetadata.getColumnDataType(columnName);
      admin.addNewColumnToTable(
          coordinatorNamespace, Coordinator.TABLE, columnName, columnDataType);
    }
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
