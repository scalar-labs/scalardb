package com.scalar.db.storage;

import java.util.Collection;
import java.util.LinkedHashSet;

public class ImmutableLinkedHashSet<E> extends LinkedHashSet<E> {

  private boolean immutable;

  public ImmutableLinkedHashSet() {}

  public ImmutableLinkedHashSet(Collection<E> c) {
    super(c);
  }

  public ImmutableLinkedHashSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public ImmutableLinkedHashSet(int initialCapacity) {
    super(initialCapacity);
  }

  public ImmutableLinkedHashSet<E> immutable() {
    immutable = true;
    return this;
  }

  @Override
  public boolean add(E e) {
    if (!immutable) {
      return super.add(e);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    if (!immutable) {
      return super.remove(o);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends E> coll) {
    if (!immutable) {
      return super.addAll(coll);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> coll) {
    if (!immutable) {
      return super.removeAll(coll);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> coll) {
    if (!immutable) {
      return super.retainAll(coll);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    if (!immutable) {
      super.clear();
    }
    throw new UnsupportedOperationException();
  }
}
