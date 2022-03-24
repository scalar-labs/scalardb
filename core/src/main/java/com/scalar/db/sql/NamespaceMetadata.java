package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class NamespaceMetadata {

  private final String name;
  private final DistributedTransactionAdmin admin;

  NamespaceMetadata(String name, DistributedTransactionAdmin admin) {
    this.name = Objects.requireNonNull(name);
    this.admin = Objects.requireNonNull(admin);
  }

  public String getName() {
    return name;
  }

  public Map<String, TableMetadata> getTables() {
    try {
      return admin.getNamespaceTableNames(name).stream()
          .collect(
              Collectors.toMap(
                  Function.identity(), t -> new TableMetadata(name, t, getTableMetadata(t))));
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get table names in a namespace", e);
    }
  }

  public Optional<TableMetadata> getTable(String tableName) {
    com.scalar.db.api.TableMetadata tableMetadata = getTableMetadata(tableName);
    if (tableMetadata == null) {
      return Optional.empty();
    }
    return Optional.of(new TableMetadata(name, tableName, tableMetadata));
  }

  private com.scalar.db.api.TableMetadata getTableMetadata(String tableName) {
    try {
      return admin.getTableMetadata(name, tableName);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get a table metadata", e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NamespaceMetadata)) {
      return false;
    }
    NamespaceMetadata that = (NamespaceMetadata) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
