package com.scalar.db.io;

/**
 * An abstraction for storage entry's value
 *
 * @author Hiroyuki Yamada
 */
public interface Value<T> extends Comparable<T> {

  /**
   * Returns the name of the value
   *
   * @return the name of this value
   */
  String getName();

  /**
   * Creates a copy of the value with the specified name
   *
   * @param name name of a {@code Value}
   * @return a {@code Value} which has the same content of this value
   */
  Value<?> copyWith(String name);

  /**
   * Accepts a {@link ValueVisitor} to be able to be traversed
   *
   * @param v a visitor class used for traversing {@code Value}s
   */
  void accept(ValueVisitor v);
}
