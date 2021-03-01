package com.scalar.db.storage;

import java.util.Collection;
import java.util.LinkedHashSet;

public class ImmutableLinkedHashSet<E> extends LinkedHashSet<E> {

  private boolean immutable;

  private ImmutableLinkedHashSet() {}

  private ImmutableLinkedHashSet(Collection<? extends E> c) {
    super(c);
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
      return;
    }
    throw new UnsupportedOperationException();
  }

  private void makeImmutable() {
    immutable = true;
  }

  public static <E> ImmutableLinkedHashSet<E> of() {
    ImmutableLinkedHashSet<E> ret = new ImmutableLinkedHashSet<>();
    ret.makeImmutable();
    return ret;
  }

  public static <E> ImmutableLinkedHashSet<E> of(Collection<? extends E> c) {
    ImmutableLinkedHashSet<E> ret = new ImmutableLinkedHashSet<>(c);
    ret.makeImmutable();
    return ret;
  }
}
