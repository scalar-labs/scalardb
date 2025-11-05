package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;

/**
 * A manager class for handling ScalarDB operations in single CRUD operation mode.
 *
 * <p>Provides access to {@link SingleCrudOperationTransactionManager} for executing individual CRUD
 * operations in a lightweight, non-distributed manner.
 *
 * <p>This class is typically used when ScalarDB is configured in single CRUD operation mode,
 * allowing direct operations without the overhead of distributed transactions.
 */
public class ScalarDbStorageManager {

  private final SingleCrudOperationTransactionManager singleCrudOperationTransactionManager;

  /**
   * Constructs a {@code ScalarDbStorageManager} with the provided {@link
   * SingleCrudOperationTransactionManager}.
   *
   * @param manager the {@code SingleCrudOperationTransactionManager} instance to be used for
   *     performing storage operations
   */
  public ScalarDbStorageManager(SingleCrudOperationTransactionManager manager) {
    singleCrudOperationTransactionManager = manager;
  }

  public SingleCrudOperationTransactionManager getSingleCrudOperationTransactionManager() {
    return this.singleCrudOperationTransactionManager;
  }
}
