package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
   * Creates a coordinator namespace/table if it does not exist.
   *
   * @throws ExecutionException if the operation failed
   */
  public void createCoordinatorTable() throws ExecutionException {
    admin.createNamespace(coordinatorNamespace, true);
    admin.createTable(coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, true);
  }

  /**
   * Creates a coordinator namespace/table if it does not exist.
   *
   * @param options options to create namespace/table
   * @throws ExecutionException if the operation failed
   */
  public void createCoordinatorTable(Map<String, String> options) throws ExecutionException {
    admin.createNamespace(coordinatorNamespace, true, options);
    admin.createTable(
        coordinatorNamespace, Coordinator.TABLE, Coordinator.TABLE_METADATA, true, options);
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
    admin.createTable(namespace, table, convertToTransactionalTable(metadata), options);
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
   * Converts a table metadata to a transactional one
   *
   * @param tableMetadata a table metadata to be converted
   * @return a transactional table metadata
   */
  private TableMetadata convertToTransactionalTable(TableMetadata tableMetadata) {
    List<String> nonPrimaryKeyColumns =
        tableMetadata.getColumnNames().stream()
            .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
            .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
            .collect(Collectors.toList());

    // Check if the table metadata already has the transactional columns
    TRANSACTION_META_COLUMNS.forEach(
        (c, t) -> {
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
}
