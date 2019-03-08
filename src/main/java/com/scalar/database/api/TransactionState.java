package com.scalar.database.api;

/** The state of a transaction */
public enum TransactionState {
  PREPARED(1),
  DELETED(2),
  COMMITTED(3),
  ABORTED(4);

  private final int id;

  private TransactionState(final int id) {
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
    throw new IllegalArgumentException("invalid id specified.");
  }
}
