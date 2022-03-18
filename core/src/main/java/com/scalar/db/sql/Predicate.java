package com.scalar.db.sql;

public class Predicate {

  public final String columnName;
  public final Operator operator;
  public final Value value;

  private Predicate(String columnName, Operator operator, Value value) {
    this.columnName = columnName;
    this.operator = operator;
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

    public Predicate isEqualTo(Value value) {
      return new Predicate(columnName, Operator.IS_EQUAL_TO, value);
    }

    public Predicate isGreaterThan(Value value) {
      return new Predicate(columnName, Operator.IS_GREATER_THAN, value);
    }

    public Predicate isGreaterThanOrEqualTo(Value value) {
      return new Predicate(columnName, Operator.IS_GREATER_THAN_OR_EQUAL_TO, value);
    }

    public Predicate isLessThan(Value value) {
      return new Predicate(columnName, Operator.IS_LESS_THAN, value);
    }

    public Predicate isLessThanOrEqualTo(Value value) {
      return new Predicate(columnName, Operator.IS_LESS_THAN_OR_EQUAL_TO, value);
    }
  }

  public enum Operator {
    IS_EQUAL_TO,
    IS_GREATER_THAN,
    IS_GREATER_THAN_OR_EQUAL_TO,
    IS_LESS_THAN,
    IS_LESS_THAN_OR_EQUAL_TO,
  }
}
