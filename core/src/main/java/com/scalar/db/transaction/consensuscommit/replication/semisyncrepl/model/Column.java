package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nullable;

public class Column<T> {
  public final String name;
  public final T value;

  public Column(@JsonProperty("name") String name, @JsonProperty("value") @Nullable T value) {
    this.name = name;
    this.value = value;
  }

  public static Column<?> fromScalarDbColumn(com.scalar.db.io.Column<?> column) {
    return new Column<>(column.getName(), column.getValue().orElse(null));
  }

  public static com.scalar.db.io.Column<?> toScalarDbColumn(Column<?> column) {
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
      throw new AssertionError();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Column)) {
      return false;
    }
    Column<?> column = (Column<?>) o;
    return Objects.equals(name, column.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
