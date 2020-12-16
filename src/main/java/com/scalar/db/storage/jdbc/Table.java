package com.scalar.db.storage.jdbc;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class Table {
  private final String schemaPrefix;
  private final String schema;
  private final String table;

  public Table(String schema, String table) {
    this("", schema, table);
  }

  public Table(String schemaPrefix, String schema, String table) {
    if (schemaPrefix == null || schema == null || table == null) {
      throw new IllegalArgumentException();
    }
    this.schemaPrefix = schemaPrefix;
    this.schema = schema;
    this.table = table;
  }

  public String getSchemaPrefix() {
    return schemaPrefix;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  @Override
  public String toString() {
    return schemaPrefix + schema + "." + table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Table table = (Table) o;
    return schemaPrefix.equals(table.schemaPrefix)
        && schema.equals(table.schema)
        && this.table.equals(table.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaPrefix, schema, table);
  }
}
