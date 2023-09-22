package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class Key {
  public final List<Column<?>> columns;

  public Key(@JsonProperty("columns") List<Column<?>> columns) {
    this.columns = columns;
  }

  public static Key fromScalarDbKey(@Nullable com.scalar.db.io.Key key) {
    if (key == null) {
      return new Key(new ArrayList<>());
    }
    return new Key(
        key.getColumns().stream().map(Column::fromScalarDbColumn).collect(Collectors.toList()));
  }

  public static com.scalar.db.io.Key toScalarDbKey(Key key) {
    if (key == null) {
      return com.scalar.db.io.Key.of();
    }
    com.scalar.db.io.Key.Builder builder = com.scalar.db.io.Key.newBuilder();
    for (Column<?> column : key.columns) {
      builder.add(Column.toScalarDbColumn(column));
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("columns", columns).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Key)) {
      return false;
    }
    Key key = (Key) o;
    return Objects.equals(columns, key.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }
}
