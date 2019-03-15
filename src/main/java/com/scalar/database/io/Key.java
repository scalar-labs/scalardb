package com.scalar.database.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An abstraction for keys
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class Key implements Comparable<Key>, Iterable<Value> {
  private final List<Value> values;

  /**
   * Constructs a {@code Key} with the specified {@link Value}s
   *
   * @param values one or more {@link Value}s which this key is composed of
   */
  public Key(Value... values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.length);
    Arrays.stream(values).forEach(v -> this.values.add(v));
  }

  /**
   * Constructs a {@code Key} with the specified list of {@link Value}s
   *
   * @param values a list of {@link Value}s which this key is composed of
   */
  public Key(List<Value> values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.size());
    values.forEach(v -> this.values.add(v));
  }

  /**
   * Returns the list of {@code Value} which this key is composed of
   *
   * @return list of {@code Value} which this key is composed of
   */
  @Nonnull
  public List<Value> get() {
    return Collections.unmodifiableList(values);
  }

  /**
   * Returns the size of the list of {@code Value} which this key is composed of
   *
   * @return the size of list of {@code Value} which this key is composed of
   */
  public int size() {
    return values.size();
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Key} and
   *   <li>both instances have the same list of {@code Value}s
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Key)) {
      return false;
    }
    Key that = (Key) o;
    if (values.equals(that.values)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    values.forEach(v -> helper.addValue(v));
    return helper.toString();
  }

  @Override
  public Iterator<Value> iterator() {
    return values.iterator();
  }

  @Override
  public int compareTo(Key o) {
    return ComparisonChain.start()
        .compare(values, o.values, Ordering.<Value>natural().lexicographical())
        .result();
  }
}
