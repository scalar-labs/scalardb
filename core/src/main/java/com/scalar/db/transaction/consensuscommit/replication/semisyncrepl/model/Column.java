package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.util.Objects;
import javax.annotation.Nullable;

public class Column<T> {
  public final String name;
  public final T value;
  public final DataType dataType;

  public Column(
      @JsonProperty("name") String name,
      @JsonProperty("value") @Nullable T value,
      @JsonProperty("data_type") DataType dataType) {
    this.name = name;
    this.value = value;
    this.dataType = dataType;
  }

  public static Column<?> fromScalarDbColumn(com.scalar.db.io.Column<?> column) {
    return new Column<>(column.getName(), column.getValue().orElse(null), column.getDataType());
  }

  public static com.scalar.db.io.Column<?> toScalarDbColumn(Column<?> column) {
    /*
    if (column.value instanceof Integer) {
      return IntColumn.of(column.name, (int) column.value);
    } else if (column.value instanceof Long) {
      return BigIntColumn.of(column.name, (Long) column.value);
    } else if (column.value instanceof String) {
      return TextColumn.of(column.name, (String) column.value);
    } else if (column.value instanceof Boolean) {
      return BooleanColumn.of(column.name, (Boolean) column.value);
    } else if (column.value instanceof Float) {
      return FloatColumn.of(column.name, (Float) column.value);
    } else if (column.value instanceof Double) {
      return DoubleColumn.of(column.name, (Double) column.value);
    } else if (column.value instanceof ByteBuffer) {
      return BlobColumn.of(column.name, (ByteBuffer) column.value);
    } else {
      throw new AssertionError("Unexpected column type. Column:" + column);
    }
     */
    switch (column.dataType) {
      case BOOLEAN:
        if (column.value == null) {
          return BooleanColumn.ofNull(column.name);
        } else {
          return BooleanColumn.of(column.name, (Boolean) column.value);
        }
      case INT:
        if (column.value == null) {
          return IntColumn.ofNull(column.name);
        } else {
          return IntColumn.of(column.name, (int) column.value);
        }
      case BIGINT:
        if (column.value == null) {
          return BigIntColumn.ofNull(column.name);
        } else {
          return BigIntColumn.of(column.name, (long) column.value);
        }
      case FLOAT:
        if (column.value == null) {
          return FloatColumn.ofNull(column.name);
        } else {
          return FloatColumn.of(column.name, (float) column.value);
        }
      case DOUBLE:
        if (column.value == null) {
          return DoubleColumn.ofNull(column.name);
        } else {
          return DoubleColumn.of(column.name, (double) column.value);
        }
      case TEXT:
        if (column.value == null) {
          return TextColumn.ofNull(column.name);
        } else {
          return TextColumn.of(column.name, (String) column.value);
        }
      case BLOB:
        if (column.value == null) {
          return BooleanColumn.ofNull(column.name);
        } else {
          return BlobColumn.of(column.name, (byte[]) column.value);
        }
    }
    throw new AssertionError();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Column)) return false;
    Column<?> column = (Column<?>) o;
    return Objects.equals(name, column.name) && Objects.equals(value, column.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
