package com.scalar.db.storage.jdbc.query;

public class SelectWithRowNumQuery extends SimpleSelectQuery {

  private final int limit;

  public SelectWithRowNumQuery(Builder builder, int limit) {
    super(builder);
    assert limit > 0;
    this.limit = limit;
  }

  @Override
  protected String sql() {
    return "SELECT * FROM (" + super.sql() + ") WHERE ROWNUM <= " + limit;
  }
}
