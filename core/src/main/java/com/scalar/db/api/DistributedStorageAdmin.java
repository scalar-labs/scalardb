package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.Map;

/**
 * An administrative interface for distributed storage implementations.
 */
public interface DistributedStorageAdmin {

  String INDEX_NAME_PREFIX = "index";

  /**
   * Creates a new table.
   *
   * @param namespace a namespace to create
   * @param table     a table to create
   * @param metadata  a metadata to create
   * @param options   options to create
   * @throws ExecutionException if the operation failed
   */
  void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException;

  /**
   * Drops the specified table.
   *
   * @param namespace a namespace to drop
   * @param table     a table to drop
   * @throws ExecutionException if the operation failed
   */
  void dropTable(String namespace, String table) throws ExecutionException;

  /**
   * Truncates the specified table.
   *
   * @param namespace a namespace to truncate
   * @param table     a table to truncate
   * @throws ExecutionException if the operation failed
   */
  void truncateTable(String namespace, String table) throws ExecutionException;

  /**
   * Retrieves the table metadata of the specified table
   *
   * @param namespace a namespace to retrieve
   * @param table     a table to retrieve
   * @return the table metadata of the specified table. null if the table is not found.
   * @throws ExecutionException if the operation failed
   */
  TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException;

  /**
   * Closes connections to the storage.
   */
  void close();
}
