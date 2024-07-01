package com.scalar.db.storage.jdbc;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * A RdnEngineStrategy implementation for SQLite.
 *
 * <p>Namespace: Added to table prefix like `${namespace}${NAMESPACE_SEPARATOR}${tableName}`.
 *
 * <p>Error handling: SQLite has limited variety of error codes compared to other JDBC databases. We
 * need to heavily rely on error messages, which may be changed in SQLite implementation in the
 * future. Since <a
 * href="https://github.com/xerial/sqlite-jdbc#sqlite-jdbc-driver">xerial/sqlite-jdbc contains a
 * native SQLite library in JAR</a>, we should assure the real error messages in
 * RdbEngineStrategyTest.
 */
class RdbEngineSqlite implements RdbEngineStrategy {
  private static final String NAMESPACE_SEPARATOR = "$";

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // Error code: SQLITE_ERROR (1)
    // Message: SQL error or missing database (table XXX already exists)

    return e.getErrorCode() == 1
        && e.getMessage().contains("(table")
        && e.getMessage().endsWith("already exists)");
  }

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // Error code: SQLITE_CONSTRAINT_PRIMARYKEY (1555)

    // Error code: SQLITE_CONSTRAINT_UNIQUE (2067)

    return ((SQLiteException) e).getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY
        || ((SQLiteException) e).getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // Error code: SQLITE_ERROR (1)
    // Message: SQL error or missing database (no such table: XXX)

    return e.getErrorCode() == 1 && e.getMessage().contains("no such table:");
  }

  @Override
  public boolean isConflict(SQLException e) {
    // Error code: SQLITE_BUSY (5)
    // Message: The database file is locked (database is locked)

    // Error code: SQLITE_BUSY (6)
    // Message: A table in the database is locked (database table is locked)

    return e.getErrorCode() == 5 || e.getErrorCode() == 6;
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // Since the "IF NOT EXISTS" syntax is used to create an index, we always return false
    return false;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INT:
        return "INT";
      case BIGINT:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case TEXT:
        return "TEXT";
      case BLOB:
        return "BLOB";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
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
    return "TEXT";
  }

  @Override
  public DataType getDataTypeForScalarDb(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
    throw new AssertionError("SQLite is not supported");
  }

  /**
   * Takes JDBC column information and returns a corresponding ScalarDB data type.
   *
   * @param type A JDBC type.
   * @param typeName A JDBC column type name.
   * @param columnSize A JDBC column size.
   * @param digits A JDBC column digits.
   * @param columnDescription A JDBC column description.
   * @return A corresponding ScalarDB data type.
   */
  @Override
  public DataType getDataTypeForScalarDbLeniently(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INTEGER:
        if (typeName.equalsIgnoreCase("int")) {
          return DataType.INT;
        } else if (typeName.equalsIgnoreCase("boolean")) {
          return DataType.BOOLEAN;
        } else if (typeName.equalsIgnoreCase("bigint")) {
          return DataType.BIGINT;
        }
        break;
      case VARCHAR:
        if (typeName.equalsIgnoreCase("text")) {
          return DataType.TEXT;
        } else if (typeName.equalsIgnoreCase("blob")) {
          return DataType.BLOB;
        }
        break;
      case FLOAT:
        if (typeName.equalsIgnoreCase("float")) {
          return DataType.FLOAT;
        } else if (typeName.equalsIgnoreCase("double")) {
          return DataType.DOUBLE;
        }
        break;
      default:
        break;
    }
    throw new IllegalArgumentException(
        String.format(
            "Unexpected data type. JDBC type: %s, Type name: %s, Column size: %d, Column digits: %d, Column desc: %s",
            type, typeName, columnSize, digits, columnDescription));
  }

  @Override
  public boolean isValidNamespaceOrTableName(String namespaceOrTableName) {
    return !namespaceOrTableName.contains(NAMESPACE_SEPARATOR);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "TRUE" : "FALSE";
  }

  @Override
  public String[] createSchemaSqls(String fullSchema) {
    // Do nothing, In SQLite storage, namespace will be added to table names as prefix along with
    // underscore
    // separator.
    return new String[0];
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String fullSchema) {
    return createSchemaSqls(fullSchema);
  }

  /**
   * @param hasDescClusteringOrder Ignored. SQLite cannot handle key order.
   * @see <a href="https://www.sqlite.org/syntax/table-constraint.html">table-constraint</a>
   */
  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + "))";
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    // do nothing
    return new String[] {};
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // Namespace is never created
    return false;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    // Do nothing. Metadata schema is just a prefix to the metadata table in the SQLite
    // implementation.
    return null;
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    // Do nothing. Namespace is just a table prefix in the SQLite implementation.
    return null;
  }

  @Override
  public String truncateTableSql(String namespace, String table) {
    // SQLite does not support TRUNCATE TABLE statement.
    return "DELETE FROM " + encloseFullTableName(namespace, table);
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace) {
    throw new AssertionError("DropNamespace never happen in SQLite implementation");
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    throw new AssertionError(
        "SQLite does not require changes in column data types when making indices");
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return enclose(schema + NAMESPACE_SEPARATOR + table);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithLimitQuery(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new InsertOnConflictDoUpdateQuery(builder);
  }

  @Override
  public Driver getDriver() {
    return new org.sqlite.JDBC();
  }

  @Override
  public boolean isImportable() {
    return false;
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? null : escape;
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS");
  }

  @Override
  @Nullable
  public String getSchemaName(String namespace) {
    return null;
  }

  @Override
  public String rawTableName(String namespace, String table) {
    return namespace + NAMESPACE_SEPARATOR + table;
  }
}
