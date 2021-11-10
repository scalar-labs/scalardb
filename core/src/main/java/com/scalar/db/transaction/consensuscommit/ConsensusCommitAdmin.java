package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitAdmin {

  private static final ImmutableMap<String, DataType> TRANSACTION_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.ID, DataType.TEXT)
          .put(Attribute.STATE, DataType.INT)
          .put(Attribute.VERSION, DataType.INT)
          .put(Attribute.PREPARED_AT, DataType.BIGINT)
          .put(Attribute.COMMITTED_AT, DataType.BIGINT)
          .put(Attribute.BEFORE_ID, DataType.TEXT)
          .put(Attribute.BEFORE_STATE, DataType.INT)
          .put(Attribute.BEFORE_VERSION, DataType.INT)
          .put(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
          .put(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
          .build();

  private final DistributedStorageAdmin admin;
  private final String coordinatorNamespace;

  public ConsensusCommitAdmin(DistributedStorageAdmin admin, ConsensusCommitConfig config) {
    this.admin = admin;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
  }

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @throws ExecutionException if the operation failed
   */
  public void createCoordinatorTable() throws ExecutionException {
    admin.createNamespace(coordinatorNamespace, true);
    admin.createTable(coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, true);
  }

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @param options options to create namespace and table
   * @throws ExecutionException if the operation failed
   */
  public void createCoordinatorTable(Map<String, String> options) throws ExecutionException {
    admin.createNamespace(coordinatorNamespace, true, options);
    admin.createTable(
        coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, true, options);
  }

  /**
   * Truncates a coordinator table.
   *
   * @throws ExecutionException if the operation failed
   */
  public void truncateCoordinatorTable() throws ExecutionException {
    admin.truncateTable(coordinatorNamespace, Coordinator.TABLE);
  }

  /**
   * Drops a coordinator namespace and table.
   *
   * @throws ExecutionException if the operation failed
   */
  public void dropCoordinatorTable() throws ExecutionException {
    admin.dropTable(coordinatorNamespace, Coordinator.TABLE);
    admin.dropNamespace(coordinatorNamespace);
  }

  /**
   * Return true if a coordinator table exists.
   *
   * @return true if a coordinator table exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  public boolean coordinatorTableExists() throws ExecutionException {
    return admin.tableExists(coordinatorNamespace, Coordinator.TABLE);
  }

  /**
   * Creates a new transactional table.
   *
   * @param namespace a namespace already created
   * @param table a table to create
   * @param metadata a metadata to create
   * @param options options to create
   * @throws ExecutionException if the operation failed
   */
  public void createTransactionalTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(namespace, table, buildTransactionalTableMetadata(metadata), options);
  }

  /**
   * Creates a new transactional table.
   *
   * @param namespace an existing namespace
   * @param table a table to create
   * @param metadata a metadata to create
   * @param ifNotExists if set to true, the table will be created only if it does not exist already.
   *     If set to false, it will try to create the table but may throw an exception if it already
   *     exists
   * @param options options to create
   * @throws ExecutionException if the operation failed
   */
  public void createTransactionalTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && admin.getNamespaceTableNames(namespace).contains(table)) {
      return;
    }
    createTransactionalTable(namespace, table, metadata, options);
  }

  /**
   * Creates a new transactional table.
   *
   * @param namespace an existing namespace
   * @param table a table to create
   * @param metadata a metadata to create
   * @param ifNotExists if set to true, the table will be created only if it does not exist already.
   *     If set to false, it will try to create the table but may throw an exception if it already
   *     exists
   * @throws ExecutionException if the operation failed
   */
  public void createTransactionalTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    if (ifNotExists && admin.getNamespaceTableNames(namespace).contains(table)) {
      return;
    }
    createTransactionalTable(namespace, table, metadata, Collections.emptyMap());
  }

  /**
   * Creates a new transactional table.
   *
   * @param namespace an existing namespace
   * @param table a table to create
   * @param metadata a metadata to create
   * @throws ExecutionException if the operation failed
   */
  public void createTransactionalTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    createTransactionalTable(namespace, table, metadata, Collections.emptyMap());
  }

  /**
   * Builds a transactional table metadata based on the specified table metadata.
   *
   * @param tableMetadata the base table metadata to build a transactional table metadata
   * @return a transactional table metadata based on the table metadata
   */
  public static TableMetadata buildTransactionalTableMetadata(TableMetadata tableMetadata) {
    List<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(tableMetadata);

    // Check if the table metadata already has the transactional columns
    TRANSACTION_META_COLUMNS
        .keySet()
        .forEach(
            c -> {
              if (tableMetadata.getColumnNames().contains(c)) {
                throw new IllegalArgumentException(
                    "column \"" + c + "\" is reserved as transaction metadata");
              }
            });
    nonPrimaryKeyColumns.forEach(
        c -> {
          String beforePrefixed = Attribute.BEFORE_PREFIX + c;
          if (tableMetadata.getColumnNames().contains(beforePrefixed)) {
            throw new IllegalArgumentException(
                "non-primary key column with the \""
                    + Attribute.BEFORE_PREFIX
                    + "\" prefix, \""
                    + beforePrefixed
                    + "\", is reserved as transaction metadata");
          }
        });

    // Build a transactional table metadata
    TableMetadata.Builder builder = TableMetadata.newBuilder(tableMetadata);
    TRANSACTION_META_COLUMNS.forEach(builder::addColumn);
    nonPrimaryKeyColumns.forEach(
        c -> builder.addColumn(Attribute.BEFORE_PREFIX + c, tableMetadata.getColumnDataType(c)));
    return builder.build();
  }

  /**
   * Returns whether the specified table metadata is transactional.
   *
   * <p>This method checks all the transactional meta columns including the before prefix column,
   * and if any of them is missing, it returns false.
   *
   * @param tableMetadata a table metadata
   * @return whether the table metadata is transactional
   */
  public static boolean isTransactionalTableMetadata(TableMetadata tableMetadata) {
    // if the table metadata doesn't have the transactional meta columns, it's not transactional
    for (String column : TRANSACTION_META_COLUMNS.keySet()) {
      if (!tableMetadata.getColumnNames().contains(column)) {
        return false;
      }
    }

    // if the table metadata doesn't have the before prefix columns, it's not transactional
    for (String nonPrimaryKeyColumn : getNonPrimaryKeyColumns(tableMetadata)) {
      if (TRANSACTION_META_COLUMNS.containsKey(nonPrimaryKeyColumn)) {
        continue;
      }
      // check if a column that has either the following name exists or not:
      //   - "before_" + the column name
      //   - the column name without the "before_" prefix
      // if both columns don't exist, the table metadata is not transactional
      if (!tableMetadata.getColumnNames().contains(Attribute.BEFORE_PREFIX + nonPrimaryKeyColumn)
          && !nonPrimaryKeyColumn.startsWith(Attribute.BEFORE_PREFIX)
          && !tableMetadata
              .getColumnNames()
              .contains(nonPrimaryKeyColumn.substring(Attribute.BEFORE_PREFIX.length()))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> getNonPrimaryKeyColumns(TableMetadata tableMetadata) {
    return tableMetadata.getColumnNames().stream()
        .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
        .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
        .collect(Collectors.toList());
  }

  /**
   * Removes transactional meta columns from the specified table metadata.
   *
   * @param tableMetadata a transactional table metadata
   * @return a table metadata without transactional meta columns
   */
  public static TableMetadata removeTransactionalMetaColumns(TableMetadata tableMetadata) {
    Set<String> transactionMetaColumns = new HashSet<>(TRANSACTION_META_COLUMNS.keySet());
    transactionMetaColumns.addAll(
        tableMetadata.getColumnNames().stream()
            .filter(c -> c.startsWith(Attribute.BEFORE_PREFIX))
            .filter(
                c ->
                    tableMetadata
                        .getColumnNames()
                        .contains(c.substring(Attribute.BEFORE_PREFIX.length())))
            .collect(Collectors.toSet()));

    TableMetadata.Builder builder = TableMetadata.newBuilder();
    tableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKey);
    tableMetadata
        .getClusteringKeyNames()
        .forEach(c -> builder.addClusteringKey(c, tableMetadata.getClusteringOrder(c)));
    tableMetadata.getColumnNames().stream()
        .filter(c -> !transactionMetaColumns.contains(c))
        .forEach(c -> builder.addColumn(c, tableMetadata.getColumnDataType(c)));
    tableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndex);
    return builder.build();
  }
}
