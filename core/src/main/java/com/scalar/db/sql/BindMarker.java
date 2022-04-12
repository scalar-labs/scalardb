package com.scalar.db.sql;

public interface BindMarker extends Term {

  static BindMarker positional() {
    return PositionalBindMarker.INSTANCE;
  }

  static BindMarker named(String name) {
    return new NamedBindMarker(name);
  }
}
