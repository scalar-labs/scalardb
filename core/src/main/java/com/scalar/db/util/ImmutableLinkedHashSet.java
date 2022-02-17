package com.scalar.db.util;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ImmutableLinkedHashSet<E> extends LinkedHashSet<E> {

  private final ImmutableSet<E> data;

  public ImmutableLinkedHashSet(Collection<? extends E> data) {
    super(0);
    this.data = ImmutableSet.copyOf(data);
  }

  @Override
  public Spliterator<E> spliterator() {
    return data.spliterator();
  }

  @Override
  public Iterator<E> iterator() {
    return data.iterator();
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public boolean isEmpty() {
    return data.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return data.contains(o);
  }

  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Set)) {
      return false;
    }
    return data.equals(o);
  }

  @Override
  public int hashCode() {
    return data.hashCode();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    return data.toArray();
  }

  @Override
  public <T> T[] toArray(@Nonnull T[] a) {
    return data.toArray(a);
  }

  @Override
  public boolean containsAll(@Nonnull Collection<?> c) {
    return data.containsAll(c);
  }

  @Override
  public boolean addAll(@Nonnull Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public boolean removeIf(Predicate<? super E> filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<E> stream() {
    return data.stream();
  }

  @Override
  public Stream<E> parallelStream() {
    return data.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super E> action) {
    data.forEach(action);
  }
}
