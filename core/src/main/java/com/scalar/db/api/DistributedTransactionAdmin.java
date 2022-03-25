package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;

/**
 * An administrative interface for distributed transaction implementations. The user can execute
 * administrative operations with it like createNamespace/createTable/getTableMetadata.
 */
public interface DistributedTransactionAdmin extends Admin {

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @param options options to create namespace and table
   * @throws ExecutionException if the operation failed
   */
  void createCoordinatorNamespaceAndTable(Map<String, String> options) throws ExecutionException;

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @param ifNotExists if set to true, the coordinator table will be created only if it does not
   *     exist already. If set to false, it will try to create the coordinator table but may throw
   *     an exception if it already exists
   * @param options options to create namespace and table
   * @throws ExecutionException if the operation failed
   */
  default void createCoordinatorNamespaceAndTable(boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && coordinatorTableExists()) {
      return;
    }
    createCoordinatorNamespaceAndTable(options);
  }

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @param ifNotExists if set to true, the coordinator table will be created only if it does not
   *     exist already. If set to false, it will try to create the coordinator table but may throw
   *     an exception if it already exists
   * @throws ExecutionException if the operation failed
   */
  default void createCoordinatorNamespaceAndTable(boolean ifNotExists) throws ExecutionException {
    if (ifNotExists && coordinatorTableExists()) {
      return;
    }
    createCoordinatorNamespaceAndTable(Collections.emptyMap());
  }

  /**
   * Creates a coordinator namespace and table if it does not exist.
   *
   * @throws ExecutionException if the operation failed
   */
  default void createCoordinatorNamespaceAndTable() throws ExecutionException {
    createCoordinatorNamespaceAndTable(Collections.emptyMap());
  }

  /**
   * Drops a coordinator namespace and table.
   *
   * @throws ExecutionException if the operation failed
   */
  void dropCoordinatorNamespaceAndTable() throws ExecutionException;

  /**
   * Drops a coordinator namespace and table.
   *
   * @param ifExists if set to true, the coordinator table will be dropped only if it exists. If set
   *     to false, it will try to drop the coordinator table but may throw an exception if it does
   *     not exist
   * @throws ExecutionException if the operation failed
   */
  default void dropCoordinatorNamespaceAndTable(boolean ifExists) throws ExecutionException {
    if (ifExists && !coordinatorTableExists()) {
      return;
    }
    dropCoordinatorNamespaceAndTable();
  }

  /**
   * Truncates a coordinator table.
   *
   * @throws ExecutionException if the operation failed
   */
  void truncateCoordinatorTable() throws ExecutionException;

  /**
   * Returns true if a coordinator table exists.
   *
   * @return true if a coordinator table exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  boolean coordinatorTableExists() throws ExecutionException;

  /** Closes connections to the storage. */
  void close();
}
