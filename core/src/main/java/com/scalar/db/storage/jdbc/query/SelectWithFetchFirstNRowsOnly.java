package com.scalar.db.storage.jdbc.query;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SelectWithFetchFirstNRowsOnly extends SimpleSelectQuery {

  private final int limit;

  public SelectWithFetchFirstNRowsOnly(Builder builder, int limit) {
    super(builder);
    assert limit > 0;
    this.limit = limit;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @Override
  public String sql() {
    return super.sql() + " FETCH FIRST " + limit + " ROWS ONLY";
  }
}
