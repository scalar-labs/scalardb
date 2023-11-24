package com.scalar.db.storage.jdbc.query;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SelectWithLimitQuery extends SimpleSelectQuery {

  private final int limit;

  public SelectWithLimitQuery(Builder builder, int limit) {
    super(builder);
    assert limit > 0;
    this.limit = limit;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @Override
  public String sql() {
    return super.sql() + " LIMIT " + limit;
  }
}
