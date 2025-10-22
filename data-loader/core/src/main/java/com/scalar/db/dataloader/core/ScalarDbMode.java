package com.scalar.db.dataloader.core;

/**
 * The available modes a ScalarDB instance can run in. Determines how ScalarDB interacts with the
 * underlying database.
 */
public enum ScalarDbMode {

  /**
   * Storage mode: Operates directly on the underlying storage engine without transactional
   * guarantees. Suitable for raw data access and simple CRUD operations.
   */
  STORAGE,

  /**
   * Transaction mode: Provides transaction management with ACID guarantees across multiple
   * operations. Suitable for applications that require consistency and atomicity.
   */
  TRANSACTION
}
