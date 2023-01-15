package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeIntoQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineOracle implements RdbEngineStrategy {

  @Override
  public void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE USER " + fullNamespace + " IDENTIFIED BY \"oracle\"");
    execute(connection, "ALTER USER " + fullNamespace + " quota unlimited on USERS");
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + ")) ROWDEPENDENCIES"; // add ROWDEPENDENCIES to the table to improve the performance
  }

  @SuppressFBWarnings({"SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
  @Override
  public void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException {
    // Set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
    // performance
    String alterTableStatement =
        "ALTER TABLE " + encloseFullTableName(schema, table) + " INITRANS 3 MAXTRANS 255";
    execute(connection, alterTableStatement);

    if (hasDescClusteringOrder) {
      // Create a unique index for the clustering orders
      String createUniqueIndexStatement =
          "CREATE UNIQUE INDEX "
              + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
              + " ON "
              + encloseFullTableName(schema, table)
              + " ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + ")";
      execute(connection, createUniqueIndexStatement);
    }
  }

  @Override
  public void createMetadataTableIfNotExistsExecute(
      Connection connection, String createTableStatement) throws SQLException {
    try {
      execute(connection, createTableStatement);
    } catch (SQLException e) {
      // Suppress the exception thrown when the table already exists
      if (!isDuplicateTableError(e)) {
        throw e;
      }
    }
  }

  @Override
  public void createMetadataSchemaIfNotExists(Connection connection, String metadataSchema)
      throws SQLException {
    try {
      execute(connection, "CREATE USER " + enclose(metadataSchema) + " IDENTIFIED BY \"oracle\"");
    } catch (SQLException e) {
      // Suppress the exception thrown when the user already exists
      if (!isDuplicateUserError(e)) {
        throw e;
      }
    }
    execute(connection, "ALTER USER " + enclose(metadataSchema) + " quota unlimited on USERS");
  }

  @Override
  public void deleteMetadataSchema(Connection connection, String metadataSchema)
      throws SQLException {
    execute(connection, "DROP USER " + enclose(metadataSchema));
  }

  @Override
  public void dropNamespace(BasicDataSource dataSource, String namespace)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, "DROP USER " + enclose(namespace));
    } catch (SQLException e) {
      throw new ExecutionException(String.format("error dropping the user %s", namespace), e);
    }
  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM " + enclose("ALL_USERS") + " WHERE " + enclose("USERNAME") + " = ?";
  }

  @Override
  public void alterColumnType(
      Connection connection, String namespace, String table, String columnName, String columnType)
      throws SQLException {
    String alterColumnStatement =
        "ALTER TABLE "
            + encloseFullTableName(namespace, table)
            + " MODIFY ( "
            + enclose(columnName)
            + " "
            + columnType
            + " )";
    execute(connection, alterColumnStatement);
  }

  @Override
  public void tableExistsInternalExecuteTableCheck(Connection connection, String fullTableName)
      throws SQLException {
    String tableExistsStatement = "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROWS ONLY";
    execute(connection, tableExistsStatement);
  }

  @Override
  public void dropIndexExecute(Connection connection, String schema, String table, String indexName)
      throws SQLException {
    String dropIndexStatement = "DROP INDEX " + enclose(indexName);
    execute(connection, dropIndexStatement);
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithFetchFirstNRowsOnly(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new MergeIntoQuery(builder);
  }

  @Override
  public RdbEngine getRdbEngine() {
    return RdbEngine.ORACLE;
  }

  @Override
  public boolean isDuplicateUserError(SQLException e) {
    // ORA-01920: user name 'string' conflicts with another user or role name
    return e.getErrorCode() == 1920;
  }

  @Override
  public boolean isDuplicateSchemaError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // ORA-00955: name is already used by an existing object
    return e.getErrorCode() == 955;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // ORA-00942: Table or view does not exist
    return e.getErrorCode() == 942;
  }

  @Override
  public boolean isConflictError(SQLException e) {
    // ORA-08177: can't serialize access for this transaction
    // ORA-00060: deadlock detected while waiting for resource
    return e.getErrorCode() == 8177 || e.getErrorCode() == 60;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "NUMBER(19)";
      case BLOB:
        return "RAW(2000)";
      case BOOLEAN:
        return "NUMBER(1)";
      case DOUBLE:
        return "BINARY_DOUBLE";
      case FLOAT:
        return "BINARY_FLOAT";
      case INT:
        return "INT";
      case TEXT:
        return "VARCHAR2(4000)";
      default:
        assert false;
        return null;
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "VARCHAR2(64)";
      case BLOB:
        return "RAW(64)";
      default:
        return null;
    }
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.BIT;
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
    return String.format("VARCHAR2(%s)", charLength);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }
}
