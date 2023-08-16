package com.scalar.db.transaction.consensuscommit.replication.semisync.columns;

import javax.annotation.Nullable;

public class Column<T> {
  public final String name;
  public final T value;

  public Column(String name, @Nullable T value) {
    this.name = name;
    this.value = value;
  }

  public static Column<?> of(com.scalar.db.io.Column<?> src) {
    return new Column<>(src.getName(), src.getValue().orElse(null));
  }
}
