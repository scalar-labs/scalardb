package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.RdbEngine;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;

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
        "CREATE SCHEMA " + TestUtils.getMetadataFullSchema(namespacePrefix, rdbEngine));
  }

  @Override
  public List<String> dropMetadataSchemaStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "DROP SCHEMA " + TestUtils.getMetadataFullSchema(namespacePrefix, rdbEngine));
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "CREATE TABLE "
            + TestUtils.getMetadataFullTableName(namespacePrefix, rdbEngine)
            + "("
            + enclose("full_table_name", rdbEngine)
            + " VARCHAR(128),"
            + enclose("column_name", rdbEngine)
            + " VARCHAR(128),"
            + enclose("data_type", rdbEngine)
            + " VARCHAR(20) NOT NULL,"
            + enclose("key_type", rdbEngine)
            + " VARCHAR(20),"
            + enclose("clustering_order", rdbEngine)
            + " VARCHAR(10),"
            + enclose("indexed", rdbEngine)
            + " BOOLEAN NOT NULL,"
            + enclose("index_order", rdbEngine)
            + " VARCHAR(10),"
            + enclose("ordinal_position", rdbEngine)
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose("full_table_name", rdbEngine)
            + ", "
            + enclose("column_name", rdbEngine)
            + "))");
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> namespacePrefix) {
    return baseStatements.insertMetadataStatements(namespacePrefix, rdbEngine);
  }

  @Override
  public List<String> dropMetadataTableStatements(Optional<String> namespacePrefix) {
    return Collections.singletonList(
        "DROP TABLE " + TestUtils.getMetadataFullTableName(namespacePrefix, rdbEngine));
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
