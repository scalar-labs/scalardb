package com.scalar.database.api;

/** Flag to represent the state of a record in a transaction. */
public enum TransactionState {
  PREPARED(1),
  DELETED(2),
  COMMITTED(3),
  ABORTED(4);

  private final int id;

  private TransactionState(final int id) {
    this.id = id;
  }

  /**
   * Returns the id of a {@code TransactionState}.
   *
   * @return an integer representing a {@code TransactionState}
   */
  public int get() {
    return id;
  }

  /**
   * Returns the {@code TransactionState} represented by the given integer.
   *
   * @param id the integer representation of a {@code TransactionState}
   * @return the {@code TransactionState} represented by the given integer
   */
  public static TransactionState getInstance(int id) {
    for (TransactionState state : TransactionState.values()) {
      if (state.get() == id) {
        return state;
      }
    }
    throw new IllegalArgumentException("invalid id specified.");
  }
}
