package com.scalar.db.api;

import java.util.Map;

/** An administrative interface for distributed storage implementations. */
public interface DistributedStorageAdmin {

  /**
   * Creates a new table.
   *
   * @param namespace a namespace to create
   * @param table a table to create
   * @param metadata a metadata to create
   * @param options options to create
   */
  void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options);

  /**
   * Drops the specified table.
   *
   * @param namespace a namespace to drop
   * @param table a table to drop
   */
  void dropTable(String namespace, String table);

  /**
   * Truncates the specified table.
   *
   * @param namespace a namespace to truncate
   * @param table a table to truncate
   */
  void truncateTable(String namespace, String table);

  /**
   * Retrieves the table metadata of the specified table
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return the table metadata of the specified table
   */
  TableMetadata getTableMetadata(String namespace, String table);

  /** Closes connections to the storage. */
  void close();
}
