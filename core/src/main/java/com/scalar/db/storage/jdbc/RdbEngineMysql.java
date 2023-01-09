package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RdbEngineMysql extends RdbEngineStrategy {

  @Override
  protected void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE SCHEMA " + fullNamespace + " character set utf8 COLLATE utf8_bin");
  }

  @Override
  protected String createTableInternalPrimaryKeyClause(
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
  protected void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata) {
    // do nothing
  }

  @Override
  void createMetadataTableIfNotExistsExecute(Connection connection, String createTableStatement) throws SQLException {
    String createTableIfNotExistsStatement =
        createTableStatement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
    execute(connection, createTableIfNotExistsStatement);
  }

  @Override
  void createMetadataSchemaIfNotExists(Connection connection, String metadataSchema) throws SQLException {
    execute(connection, "CREATE SCHEMA IF NOT EXISTS " + enclose(metadataSchema));
  }

  @Override
  void deleteMetadataSchema(Connection connection, String metadataSchema) throws SQLException {
    execute(connection, "DROP SCHEMA " + enclose(metadataSchema));
  }

  @Override
  void dropNamespace(BasicDataSource dataSource, String namespace) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, "DROP SCHEMA " + enclose(namespace));
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format("error dropping the schema %s", namespace),
          e);
    }
  }

  @Override
  String namespaceExistsStatement() {
    return
        "SELECT 1 FROM "
            + encloseFullTableName("information_schema", "schemata")
            + " WHERE "
            + enclose("schema_name")
            + " = ?";
  }

  @Override
  boolean isDuplicateUserError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateSchemaError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateTableError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateKeyError(SQLException e) {
    // Error number: 1022; Symbol: ER_DUP_KEY; SQLSTATE: 23000
    // Message: Can't write; duplicate key in table '%s'
    // etc... See: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>
    return e.getSQLState().equals("23000");
  }

  @Override
  boolean isUndefinedTableError(SQLException e) {
    // Error number: 1049; Symbol: ER_BAD_DB_ERROR; SQLSTATE: 42000
    // Message: Unknown database '%s'

    // Error number: 1146; Symbol: ER_NO_SUCH_TABLE; SQLSTATE: 42S02
    // Message: Table '%s.%s' doesn't exist

    return e.getErrorCode() == 1049 || e.getErrorCode() == 1146;
  }

  @Override
  public boolean isConflictError(SQLException e) {
    // Error number: 1213; Symbol: ER_LOCK_DEADLOCK; SQLSTATE: 40001
    // Message: Deadlock found when trying to get lock; try restarting transaction

    // Error number: 1205; Symbol: ER_LOCK_WAIT_TIMEOUT; SQLSTATE: HY000
    // Message: Lock wait timeout exceeded; try restarting transaction

    return e.getErrorCode() == 1213 || e.getErrorCode() == 1205;
  }

  @Override
  public RdbEngine getRdbEngine() {
    return RdbEngine.MYSQL;
  }

  @Override
  protected String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "LONGBLOB";
      case BOOLEAN:
        return "BOOLEAN";
      case DOUBLE:
      case FLOAT:
        return "DOUBLE";
      case INT:
        return "INT";
      case TEXT:
        return "LONGTEXT";
      default:
        assert false;
        return null;
    }
  }

  @Override
  String getTextType(int charLength) {
    return String.format("VARCHAR(%s)", charLength);
  }

  @Override
  String computeBooleanValue(boolean value) {
    return value ? "true" : "false";
  }
}
