package com.scalar.db.dataloader.core;

/**
 * The available transaction modes for data import operations. Determines how data is imported based
 * on the transaction manager configuration.
 */
public enum TransactionMode {

  /**
   * Single-crud mode: Each operation is committed immediately without separate transaction
   * management. Used when the transaction manager is configured as single-crud operation mode. Each
   * CRUD operation is executed and committed individually.
   */
  SINGLE_CRUD,

  /**
   * Consensus commit mode: Operations are grouped into transaction batches with managed
   * transactions. Used with standard transaction managers that provide ACID guarantees across
   * multiple operations. Enables commit coordination across multiple operations.
   */
  CONSENSUS_COMMIT
}
