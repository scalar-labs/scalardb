package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;

public enum TransactionState {
  PREPARED(1),
  DELETED(2),
  COMMITTED(3),
  ABORTED(4),
  UNKNOWN(5);

  private final int id;

  TransactionState(final int id) {
    this.id = id;
  }

  public int get() {
    return id;
  }

  public static TransactionState getInstance(int id) {
    for (TransactionState state : TransactionState.values()) {
      if (state.get() == id) {
        return state;
      }
    }
    throw new IllegalArgumentException(
        CoreError.TRANSACTION_STATE_INSTANTIATION_ERROR_INVALID_ID.buildMessage(id));
  }
}
