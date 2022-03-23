package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NamespaceMetadata {

  private final String name;
  private final DistributedTransactionAdmin admin;

  public NamespaceMetadata(String name, DistributedTransactionAdmin admin) {
    this.name = name;
    this.admin = admin;
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
}
