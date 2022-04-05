package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Projection {

  public final String columnName;
  @Nullable public final String alias;

  private Projection(String columnName, @Nullable String alias) {
    this.columnName = Objects.requireNonNull(columnName);
    this.alias = alias;
  }

  public Projection as(String alias) {
    return new Projection(columnName, alias);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnName", columnName)
        .add("alias", alias)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Projection)) {
      return false;
    }
    Projection that = (Projection) o;
    return Objects.equals(columnName, that.columnName) && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, alias);
  }

  public static Projection column(String columnName) {
    return new Projection(columnName, null);
  }
}
