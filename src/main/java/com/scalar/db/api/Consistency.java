package com.scalar.db.api;

/**
 * Consistency level set in an {@link Operation}.
 *
 * @author Hiroyuki Yamada
 */
public enum Consistency {
  /**
   * Sequential consistency. With this consistency, it assumes that the underlining storage
   * implementation makes all operations appear to take effect in some sequential order, and the
   * operations of each individual process appear in this sequence.
   */
  SEQUENTIAL,
  /**
   * Eventual consistency. With this consistency, it assumes that the underlining storage
   * implementation makes all operations take effect eventually.
   */
  EVENTUAL,
  /**
   * Linearizable consistency. With this consistency, it assumes that the underlining storage
   * implementation makes each operation appears to take effect atomically at some point between its
   * invocation and completion.
   */
  LINEARIZABLE,
}
