package com.scalar.db.transaction.singlecrudoperation;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.common.DecoratedDistributedTransactionManager;

public final class SingleCrudOperationTransactionUtils {

  private SingleCrudOperationTransactionUtils() {}

  public static boolean isSingleCrudOperationTransactionManager(
      DistributedTransactionManager transactionManager) {
    if (transactionManager instanceof DecoratedDistributedTransactionManager) {
      return ((DecoratedDistributedTransactionManager) transactionManager)
              .getOriginalTransactionManager()
          instanceof SingleCrudOperationTransactionManager;
    }
    return transactionManager instanceof SingleCrudOperationTransactionManager;
  }

  public static SingleCrudOperationTransactionManager getSingleCrudOperationTransactionManager(
      DistributedTransactionManager transactionManager) {
    assert isSingleCrudOperationTransactionManager(transactionManager);

    if (transactionManager instanceof DecoratedDistributedTransactionManager) {
      return (SingleCrudOperationTransactionManager)
          ((DecoratedDistributedTransactionManager) transactionManager)
              .getOriginalTransactionManager();
    }
    return (SingleCrudOperationTransactionManager) transactionManager;
  }
}
