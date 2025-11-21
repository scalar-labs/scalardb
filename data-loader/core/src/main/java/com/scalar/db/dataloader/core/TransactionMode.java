package com.scalar.db.dataloader.core;

/**
 * Defines the transaction mode for data loader operations. This determines how data operations are
 * executed within ScalarDB.
 */
public enum TransactionMode {

  /**
   * Single CRUD mode: Executes each operation independently without transactional context. Each
   * operation is atomic but there are no guarantees across multiple operations.
   */
  SINGLE_CRUD,

  /**
   * Consensus commit mode: Groups multiple operations into distributed transactions with ACID
   * guarantees. Ensures atomicity and consistency across multiple operations through consensus
   * commit protocol.
   */
  CONSENSUS_COMMIT
}
