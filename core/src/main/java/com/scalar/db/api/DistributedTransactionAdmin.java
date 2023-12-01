package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;

/**
 * An administrative interface for distributed transaction implementations. The user can execute
 * administrative operations with it like createNamespace/createTable/getTableMetadata.
 */
public interface DistributedTransactionAdmin extends Admin, AuthAdmin {

  /**
   * Creates coordinator namespace and tables.
   *
   * @param options options to create namespace and tables
   * @throws IllegalArgumentException if the coordinator namespace already exists or the coordinator
   *     tables already exist
   * @throws ExecutionException if the operation fails
   */
  void createCoordinatorTables(Map<String, String> options) throws ExecutionException;

  /**
   * Creates coordinator namespace and tables.
   *
   * @param ifNotExist if set to true, the coordinator namespace and tables will be created only if
   *     they do not exist. If set to false, it will throw an exception if they already exist
   * @param options options to create namespace and tables
   * @throws IllegalArgumentException if the coordinator namespace already exists or the coordinator
   *     tables already exist if ifNotExist is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createCoordinatorTables(boolean ifNotExist, Map<String, String> options)
      throws ExecutionException {
    if (ifNotExist && coordinatorTablesExist()) {
      return;
    }
    createCoordinatorTables(options);
  }

  /**
   * Creates coordinator namespace and tables.
   *
   * @param ifNotExist if set to true, the coordinator namespace and tables will be created only if
   *     they do not exist. If set to false, it will throw an exception if they already exist
   * @throws IllegalArgumentException if the coordinator namespace already exists or the coordinator
   *     tables already exist if ifNotExist is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createCoordinatorTables(boolean ifNotExist) throws ExecutionException {
    if (ifNotExist && coordinatorTablesExist()) {
      return;
    }
    createCoordinatorTables(Collections.emptyMap());
  }

  /**
   * Creates coordinator namespace and tables.
   *
   * @throws IllegalArgumentException if the coordinator namespace already exists or the coordinator
   *     tables already exist if ifNotExist is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createCoordinatorTables() throws ExecutionException {
    createCoordinatorTables(Collections.emptyMap());
  }

  /**
   * Drops coordinator namespace and tables.
   *
   * @throws IllegalArgumentException if the coordinator tables do not exist
   * @throws ExecutionException if the operation fails
   */
  void dropCoordinatorTables() throws ExecutionException;

  /**
   * Drops coordinator namespace and tables.
   *
   * @param ifExist if set to true, the coordinator namespace and tables will be dropped only if
   *     they exist. If set to false, it will throw an exception if they do not exist
   * @throws IllegalArgumentException if the coordinator tables do not exist if ifExist is set to
   *     false
   * @throws ExecutionException if the operation fails
   */
  default void dropCoordinatorTables(boolean ifExist) throws ExecutionException {
    if (ifExist && !coordinatorTablesExist()) {
      return;
    }
    dropCoordinatorTables();
  }

  /**
   * Truncates coordinator tables.
   *
   * @throws IllegalArgumentException if the coordinator tables do not exist
   * @throws ExecutionException if the operation fails
   */
  void truncateCoordinatorTables() throws ExecutionException;

  /**
   * Returns true if all the coordinator tables exist.
   *
   * @return true if all the coordinator tables exist, false otherwise
   * @throws ExecutionException if the operation fails
   */
  boolean coordinatorTablesExist() throws ExecutionException;

  /**
   * Repair coordinator tables which may be in an unknown state.
   *
   * @param options options to repair
   * @throws IllegalArgumentException if the coordinator tables do not exist
   * @throws ExecutionException if the operation fails
   */
  void repairCoordinatorTables(Map<String, String> options) throws ExecutionException;

  /** Closes connections to the storage. */
  void close();
}
