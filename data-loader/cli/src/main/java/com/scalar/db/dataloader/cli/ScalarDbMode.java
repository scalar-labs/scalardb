package com.scalar.db.dataloader.cli;

/**
 * The available modes a ScalarDB instance can run in. Determines how ScalarDB interacts with the
 * underlying database.
 *
 * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. This enum is maintained for
 *     CLI backward compatibility only. Internally, values are converted to {@code TransactionMode}
 *     in the core module.
 */
@Deprecated
public enum ScalarDbMode {

  /**
   * Storage mode: Operates directly on the underlying storage engine without transactional
   * guarantees. Suitable for raw data access and simple CRUD operations.
   *
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  STORAGE,

  /**
   * Transaction mode: Provides transaction management with ACID guarantees across multiple
   * operations. Suitable for applications that require consistency and atomicity.
   *
   * @deprecated As of release 3.17.0 Will be removed in release 4.0.0.
   */
  @Deprecated
  TRANSACTION
}
