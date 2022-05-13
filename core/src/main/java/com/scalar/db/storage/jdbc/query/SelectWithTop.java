package com.scalar.db.storage.jdbc.query;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SelectWithTop extends SimpleSelectQuery {

  private final int limit;

  public SelectWithTop(Builder builder, int limit) {
    super(builder);
    assert limit > 0;
    this.limit = limit;
  }

  @Override
  public String sql() {
    return new StringBuilder(super.sql()).insert(7, "TOP " + limit + " ").toString();
  }
}
