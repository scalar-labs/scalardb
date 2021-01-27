package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractStatements implements Statements {

  private final BaseStatements baseStatements;
  private final RdbEngine rdbEngine;

  public AbstractStatements(BaseStatements baseStatements) {
    this.baseStatements = baseStatements;
    rdbEngine = getRdbEngine();
  }

  private RdbEngine getRdbEngine() {
    if (this instanceof MySqlStatements) {
      return RdbEngine.MYSQL;
    } else if (this instanceof PostgreSqlStatements) {
      return RdbEngine.POSTGRESQL;
    } else if (this instanceof OracleStatements) {
      return RdbEngine.ORACLE;
    } else {
      return RdbEngine.SQL_SERVER;
    }
  }

  protected List<String> schemas(Optional<String> namespacePrefix) {
    return baseStatements.schemas(namespacePrefix, rdbEngine);
  }

  protected List<String> tables(Optional<String> namespacePrefix) {
    return baseStatements.tables(namespacePrefix, rdbEngine);
  }

  @Override
  public List<String> createMetadataSchemaStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "CREATE SCHEMA " + TableMetadataManager.getFullSchema(namespacePrefix));
  }

  @Override
  public List<String> dropMetadataSchemaStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "DROP SCHEMA " + TableMetadataManager.getFullSchema(namespacePrefix));
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "CREATE TABLE "
            + TableMetadataManager.getFullTableName(namespacePrefix)
            + "(full_table_name VARCHAR(128),"
            + "column_name VARCHAR(128),"
            + "data_type VARCHAR(20) NOT NULL,"
            + "key_type VARCHAR(20),"
            + "clustering_order VARCHAR(10),"
            + "indexed BOOLEAN NOT NULL,"
            + "index_order VARCHAR(10),"
            + "ordinal_position INTEGER NOT NULL,"
            + "PRIMARY KEY (full_table_name, column_name))");
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> namespacePrefix) {
    return baseStatements.insertMetadataStatements(namespacePrefix);
  }

  @Override
  public List<String> dropMetadataTableStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "DROP TABLE " + TableMetadataManager.getFullTableName(namespacePrefix));
  }

  @Override
  public List<String> createSchemaStatements(Optional<String> namespacePrefix) {
    return schemas(namespacePrefix).stream()
        .map(s -> "CREATE SCHEMA " + s)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropSchemaStatements(Optional<String> namespacePrefix) {
    return schemas(namespacePrefix).stream()
        .map(s -> "DROP SCHEMA " + s)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createTableStatements(Optional<String> namespacePrefix) {
    return baseStatements.createTableStatements(namespacePrefix, rdbEngine);
  }

  @Override
  public List<String> dropTableStatements(Optional<String> namespacePrefix) {
    return tables(namespacePrefix).stream()
        .map(t -> "DROP TABLE " + t)
        .collect(Collectors.toList());
  }
}
