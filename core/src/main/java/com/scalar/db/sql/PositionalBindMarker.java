package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class PositionalBindMarker implements BindMarker {

  static final PositionalBindMarker INSTANCE = new PositionalBindMarker();

  private PositionalBindMarker() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
