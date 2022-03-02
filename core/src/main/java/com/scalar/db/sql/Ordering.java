package com.scalar.db.sql;

public class Ordering {

  public final String columnName;
  public final Order order;

  private Ordering(String columnName, Order order) {
    this.columnName = columnName;
    this.order = order;
  }

  public static Builder column(String columnName) {
    return new Builder(columnName);
  }

  public static class Builder {
    private final String columnName;

    public Builder(String columnName) {
      this.columnName = columnName;
    }

    public Ordering asc() {
      return new Ordering(columnName, Order.ASC);
    }

    public Ordering desc() {
      return new Ordering(columnName, Order.DESC);
    }
  }
}
