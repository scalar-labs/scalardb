package com.scalar.db.storage.jdbc.query;

public class SelectWithOffsetFetchQuery extends SimpleSelectQuery {

  private final int limit;

  public SelectWithOffsetFetchQuery(Builder builder, int limit) {
    super(builder);
    assert limit > 0;
    this.limit = limit;
  }

  @Override
  protected String sql() {
    return super.sql() + " OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY";
  }
}
