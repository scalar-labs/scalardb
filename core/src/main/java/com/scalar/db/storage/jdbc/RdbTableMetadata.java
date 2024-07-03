package com.scalar.db.storage.jdbc;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.DataType;
import java.util.Objects;
import javax.annotation.Nullable;
import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
class RdbTableMetadata {
  final ImmutableList<PrimaryKeyColumn> primaryKeyColumns;
  @Nullable final ImmutableSet<IndexColumn> indexColumns;
  final ImmutableMap<String, DataType> columns;

  RdbTableMetadata(
      ImmutableList<PrimaryKeyColumn> primaryKeyColumns,
      @Nullable ImmutableSet<IndexColumn> indexColumns,
      ImmutableMap<String, DataType> columns) {
    this.primaryKeyColumns = primaryKeyColumns;
    this.indexColumns = indexColumns;
    this.columns = columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RdbTableMetadata)) return false;
    RdbTableMetadata that = (RdbTableMetadata) o;
    return Objects.equals(primaryKeyColumns, that.primaryKeyColumns)
        && Objects.equals(indexColumns, that.indexColumns)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primaryKeyColumns, indexColumns, columns);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("primaryKeyColumns", primaryKeyColumns)
        .add("indexColumns", indexColumns)
        .add("columns", columns)
        .toString();
  }

  enum SortOrder {
    ASC,
    DESC,
    UNKNOWN;
  }

  @Immutable
  static class PrimaryKeyColumn {
    final String name;
    final SortOrder sortOrder;

    public PrimaryKeyColumn(String name, SortOrder sortOrder) {
      this.name = name;
      this.sortOrder = sortOrder;
    }

    public PrimaryKeyColumn(String name) {
      // TODO: Revisit this default.
      this(name, SortOrder.ASC);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PrimaryKeyColumn)) return false;
      PrimaryKeyColumn that = (PrimaryKeyColumn) o;
      return Objects.equals(name, that.name) && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, sortOrder);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("sortOrder", sortOrder)
          .toString();
    }
  }

  @Immutable
  static class IndexColumn {
    final String name;
    final SortOrder sortOrder;

    public IndexColumn(String name, SortOrder sortOrder) {
      this.name = name;
      this.sortOrder = sortOrder;
    }

    public IndexColumn(String name) {
      // TODO: Revisit this default.
      this(name, SortOrder.ASC);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IndexColumn)) return false;
      IndexColumn that = (IndexColumn) o;
      return Objects.equals(name, that.name) && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, sortOrder);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("sortOrder", sortOrder)
          .toString();
    }
  }
}
