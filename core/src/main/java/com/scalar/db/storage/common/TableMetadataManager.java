package com.scalar.db.storage.common;

import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;

public interface TableMetadataManager {
  /**
   * Returns a table metadata corresponding to the specified operation.
   *
   * @param operation an operation
   * @return a table metadata. null if the table is not found.
   */
  TableMetadata getTableMetadata(Operation operation);

  /**
   * Returns a table metadata corresponding to the specified namespace and table.
   *
   * @param namespace a namespace
   * @param table a table
   * @return a table metadata. null if the table is not found.
   */
  TableMetadata getTableMetadata(String namespace, String table);
}
