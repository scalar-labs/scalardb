package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Assignment {

  public final String columnName;
  public final Value value;

  private Assignment(String columnName, Value value) {
    this.columnName = Objects.requireNonNull(columnName);
    this.value = Objects.requireNonNull(value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnName", columnName)
        .add("value", value)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Assignment)) {
      return false;
    }
    Assignment that = (Assignment) o;
    return Objects.equals(columnName, that.columnName) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, value);
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
