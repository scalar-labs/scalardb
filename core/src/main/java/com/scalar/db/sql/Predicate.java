package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Predicate {

  public final String columnName;
  public final Operator operator;
  public final Term value;

  private Predicate(String columnName, Operator operator, Term value) {
    this.columnName = Objects.requireNonNull(columnName);
    this.operator = Objects.requireNonNull(operator);
    this.value = Objects.requireNonNull(value);
  }

  public Predicate replaceValue(Term newValue) {
    return new Predicate(columnName, operator, newValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnName", columnName)
        .add("operator", operator)
        .add("value", value)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Predicate)) {
      return false;
    }
    Predicate predicate = (Predicate) o;
    return Objects.equals(columnName, predicate.columnName)
        && operator == predicate.operator
        && Objects.equals(value, predicate.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, operator, value);
  }

  public static Builder column(String columnName) {
    return new Builder(columnName);
  }

  public static class Builder {
    private final String columnName;

    private Builder(String columnName) {
      this.columnName = columnName;
    }

    public Predicate isEqualTo(Term value) {
      return new Predicate(columnName, Operator.EQUAL_TO, value);
    }

    public Predicate isGreaterThan(Term value) {
      return new Predicate(columnName, Operator.GREATER_THAN, value);
    }

    public Predicate isGreaterThanOrEqualTo(Term value) {
      return new Predicate(columnName, Operator.GREATER_THAN_OR_EQUAL_TO, value);
    }

    public Predicate isLessThan(Term value) {
      return new Predicate(columnName, Operator.LESS_THAN, value);
    }

    public Predicate isLessThanOrEqualTo(Term value) {
      return new Predicate(columnName, Operator.LESS_THAN_OR_EQUAL_TO, value);
    }
  }

  public enum Operator {
    EQUAL_TO,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL_TO,
    LESS_THAN,
    LESS_THAN_OR_EQUAL_TO,
  }
}
