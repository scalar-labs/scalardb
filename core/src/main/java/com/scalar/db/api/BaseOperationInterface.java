package com.scalar.db.api;

import java.util.Optional;

/** A common interface for storage operations. */
public interface BaseOperationInterface {

  /**
   * Returns the namespace for this operation
   *
   * @return an {@code Optional} with the returned namespace
   */
  Optional<String> forNamespace();

  /**
   * Returns the table name for this operation
   *
   * @return an {@code Optional} with the returned table name
   */
  Optional<String> forTable();

  /**
   * Sets the specified target namespace for this operation
   *
   * @param namespace target namespace for this operation
   * @return a {@code BaseOperationInterface} object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  BaseOperationInterface forNamespace(String namespace);

  /**
   * Sets the specified target table name for this operation
   *
   * @param tableName target table name for this operation
   * @return a {@code BaseOperationInterface} object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  BaseOperationInterface forTable(String tableName);
}
