package com.scalar.db.sql;

public interface BindMarker extends Term {

  static BindMarker of() {
    return PositionalBindMarker.INSTANCE;
  }

  static BindMarker of(String name) {
    return new NamedBindMarker(name);
  }
}
