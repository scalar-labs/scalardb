package com.scalar.db.sql;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Assignment {

  public final String columnName;
  public final Value value;

  private Assignment(String columnName, Value value) {
    this.columnName = columnName;
    this.value = value;
  }

  public static Builder column(String columnName) {
    return new Builder(columnName);
  }

  public static class Builder {
    private final String columnName;

    private Builder(String columnName) {
      this.columnName = columnName;
    }

    public Assignment value(Value value) {
      return new Assignment(columnName, value);
    }
  }
}
