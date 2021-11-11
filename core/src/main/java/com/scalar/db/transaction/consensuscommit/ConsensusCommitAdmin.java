package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.buildTransactionalTableMetadata;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitAdmin {

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
}
