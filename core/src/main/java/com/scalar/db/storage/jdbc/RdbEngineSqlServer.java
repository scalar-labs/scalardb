package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithTop;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineSqlServer implements RdbEngineStrategy {

  @Override
  public String[] createNamespaceExecuteSqls(String fullNamespace) {
    return new String[]{"CREATE SCHEMA " + fullNamespace};
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    if (hasDescClusteringOrder) {
      return "PRIMARY KEY ("
          + Stream.concat(
                  metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                  metadata.getClusteringKeyNames().stream()
                      .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
              .collect(Collectors.joining(","))
          + "))";
    } else {
      return "PRIMARY KEY ("
          + Stream.concat(
                  metadata.getPartitionKeyNames().stream(),
                  metadata.getClusteringKeyNames().stream())
              .map(this::enclose)
              .collect(Collectors.joining(","))
          + "))";
    }
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDescClusteringOrder,
      String schema,
      String table,
      TableMetadata metadata) {
    // do nothing
    return new String[0];
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql;
  }

  @Override
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[]{"CREATE SCHEMA " + enclose(metadataSchema)};
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    return isDuplicateSchemaError(e);
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return "DROP SCHEMA " + enclose(metadataSchema);
  }

  @Override
  public void dropNamespace(BasicDataSource dataSource, String namespace)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, "DROP SCHEMA " + enclose(namespace));
    } catch (SQLException e) {
      throw new ExecutionException(String.format("error dropping the schema %s", namespace), e);
    }
  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM "
        + encloseFullTableName("sys", "schemas")
        + " WHERE "
        + enclose("name")
        + " = ?";
  }

  @Override
  public void alterColumnType(
      Connection connection, String namespace, String table, String columnName, String columnType)
      throws SQLException {
    // SQLServer does not require changes in column data types when making indices.
    throw new AssertionError();
  }

  @Override
  public void tableExistsInternalExecuteTableCheck(Connection connection, String fullTableName)
      throws SQLException {
    String tableExistsStatement = "SELECT TOP 1 1 FROM " + fullTableName;
    execute(connection, tableExistsStatement);
  }

  @Override
  public void dropIndexExecute(Connection connection, String schema, String table, String indexName)
      throws SQLException {
    String dropIndexStatement =
        "DROP INDEX " + enclose(indexName) + " ON " + encloseFullTableName(schema, table);
    execute(connection, dropIndexStatement);
  }

  @Override
  public boolean isDuplicateUserError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDuplicateSchemaError(SQLException e) {
    // 2714: There is already an object named '%.*ls' in the database.
    return e.getErrorCode() == 2714;
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // 2714: There is already an object named '%.*ls' in the database.
    return e.getErrorCode() == 2714;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // 23000: Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // 208: Invalid object name '%.*ls'.
    return e.getErrorCode() == 208;
  }

  @Override
  public boolean isConflictError(SQLException e) {
    // 1205: Transaction (Process ID %d) was deadlocked on %.*ls resources with another process and
    // has been chosen as the deadlock victim. Rerun the transaction.
    return e.getErrorCode() == 1205;
  }

  @Override
  public String enclose(String name) {
    return "[" + name + "]";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithTop(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new MergeQuery(builder);
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "VARBINARY(8000)";
      case BOOLEAN:
        return "BIT";
      case DOUBLE:
        return "FLOAT";
      case FLOAT:
        return "FLOAT(24)";
      case INT:
        return "INT";
      case TEXT:
        return "VARCHAR(8000) COLLATE Latin1_General_BIN";
      default:
        assert false;
        return null;
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    // PostgreSQL does not require any change in column data types when making indices.
    return null;
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT:
        return Types.INTEGER;
      case BIGINT:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
        return Types.VARCHAR;
      case BLOB:
        return Types.BLOB;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength) {
    return String.format("VARCHAR(%s)", charLength);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }
}
