package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;

/**
 * An administrative interface for the replication feature. The user can execute administrative
 * operations with it like createNamespace/createTable/getTableMetadata.
 */
public interface ReplicationAdmin {

  /**
   * Creates the replication-related namespace and tables in the replication database.
   *
   * @param options options to create namespace and tables
   * @throws ExecutionException if the operation fails
   */
  default void createReplicationTables(Map<String, String> options) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.REPLICATION_NOT_ENABLED.buildMessage());
  }

  /**
   * Creates the replication-related namespace and tables in the replication database.
   *
   * @param ifNotExist if set to true, the replication-related namespace and tables will be created
   *     only if they do not exist. If set to false, it will throw an exception if they already
   *     exist
   * @param options options to create namespace and tables
   * @throws IllegalArgumentException if the replication-related namespace or tables already exist
   *     and ifNotExist is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createReplicationTables(boolean ifNotExist, Map<String, String> options)
      throws ExecutionException {
    if (ifNotExist && replicationTablesExist()) {
      return;
    }
    createReplicationTables(options);
  }

  /**
   * Creates the replication-related namespace and tables in the replication database.
   *
   * @param ifNotExist if set to true, the replication-related namespace and tables will be created
   *     only if they do not exist. If set to false, it will throw an exception if they already
   *     exist
   * @throws IllegalArgumentException if the replication-related namespace or tables already exist
   *     and ifNotExist is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createReplicationTables(boolean ifNotExist) throws ExecutionException {
    if (ifNotExist && replicationTablesExist()) {
      return;
    }
    createReplicationTables(Collections.emptyMap());
  }

  /**
   * Creates the replication-related namespace and tables in the replication database.
   *
   * @throws ExecutionException if the operation fails
   */
  default void createReplicationTables() throws ExecutionException {
    createReplicationTables(Collections.emptyMap());
  }

  /**
   * Drops the replication-related namespace and tables in the replication database.
   *
   * @throws IllegalArgumentException if the replication-related tables do not exist
   * @throws ExecutionException if the operation fails
   */
  default void dropReplicationTables() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.REPLICATION_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops the replication-related namespace and tables in the replication database.
   *
   * @param ifExist if set to true, the replication-related namespace and tables will be dropped
   *     only if they exist. If set to false, it will throw an exception if they do not exist
   * @throws IllegalArgumentException if the replication-related tables do not exist if ifExist is
   *     set to false
   * @throws ExecutionException if the operation fails
   */
  default void dropReplicationTables(boolean ifExist) throws ExecutionException {
    if (ifExist && !replicationTablesExist()) {
      return;
    }
    dropReplicationTables();
  }

  /**
   * Truncates the replication-related tables in the replication database.
   *
   * @throws IllegalArgumentException if the replication-related tables do not exist
   * @throws ExecutionException if the operation fails
   */
  default void truncateReplicationTables() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.REPLICATION_NOT_ENABLED.buildMessage());
  }

  /**
   * Repairs the replication-related tables in the replication database which may be in an unknown
   * state.
   *
   * @param options options to repair
   * @throws IllegalArgumentException if the replication-related tables do not exist
   * @throws ExecutionException if the operation fails
   */
  default void repairReplicationTables(Map<String, String> options) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.REPLICATION_NOT_ENABLED.buildMessage());
  }

  /**
   * Checks whether all the replication-related tables exist in the replication database.
   *
   * @return true if all the replication-related tables exist, false otherwise
   * @throws ExecutionException if the operation fails
   */
  default boolean replicationTablesExist() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.REPLICATION_NOT_ENABLED.buildMessage());
  }
}
