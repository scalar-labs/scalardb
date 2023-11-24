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

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @Override
  public String sql() {
    // This inserts "TOP ${limit}" clause, specific to SqlServer, right after the "SELECT" clause of
    // the query returned by super.sql()
    return new StringBuilder(super.sql()).insert(7, "TOP " + limit + " ").toString();
  }
}
