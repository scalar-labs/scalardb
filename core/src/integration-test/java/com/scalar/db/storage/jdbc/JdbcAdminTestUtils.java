package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.executeQuery;
import static com.scalar.db.storage.jdbc.JdbcAdmin.withConnection;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.ThrowableFunction;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final HikariDataSource dataSource;
  private final boolean requiresExplicitCommit;

  public JdbcAdminTestUtils(Properties properties) {
    super(properties);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    metadataSchema = config.getMetadataSchema();
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    execute(
        "DROP TABLE "
            + rdbEngine.encloseFullTableName(metadataSchema, NamespaceMetadataService.TABLE_NAME));
  }

  @Override
  public void dropMetadataTable() throws Exception {
    dropTable(metadataSchema, TableMetadataService.TABLE_NAME);
  }

  @Override
  public void truncateNamespacesTable() throws Exception {
    String truncateTableStatement =
        rdbEngine.truncateTableSql(metadataSchema, NamespaceMetadataService.TABLE_NAME);
    execute(truncateTableStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    String truncateTableStatement =
        rdbEngine.truncateTableSql(metadataSchema, TableMetadataService.TABLE_NAME);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + rdbEngine.encloseFullTableName(metadataSchema, TableMetadataService.TABLE_NAME)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  @Override
  public void deleteMetadata(String namespace, String table) throws Exception {
    String deleteMetadataStatement =
        "DELETE FROM "
            + rdbEngine.encloseFullTableName(metadataSchema, TableMetadataService.TABLE_NAME)
            + " WHERE "
            + rdbEngine.enclose(TableMetadataService.COL_FULL_TABLE_NAME)
            + " = '"
            + getFullTableName(namespace, table)
            + "'";
    execute(deleteMetadataStatement);
  }

  /**
   * Creates an index with the specified index name.
   *
   * @param namespace the namespace of the table
   * @param table the table name
   * @param column the column name to create the index on
   * @param indexName the index name to use
   * @throws SQLException if a database error occurs
   */
  public void createIndex(String namespace, String table, String column, String indexName)
      throws SQLException {
    String sql = rdbEngine.createIndexSql(namespace, table, indexName, column);
    execute(sql);
  }

  /**
   * Drops an index with the specified index name.
   *
   * @param namespace the namespace of the table
   * @param table the table name
   * @param indexName the index name to drop
   * @throws SQLException if a database error occurs
   */
  public void dropIndex(String namespace, String table, String indexName) throws SQLException {
    String sql = rdbEngine.dropIndexSql(namespace, table, indexName);
    execute(sql);
  }

  private void execute(String sql) throws SQLException {
    withConnection(
        dataSource,
        requiresExplicitCommit,
        connection -> {
          JdbcAdmin.execute(connection, sql, requiresExplicitCommit);
        });
  }

  private void execute(String[] sqls) throws SQLException {
    withConnection(
        dataSource,
        requiresExplicitCommit,
        connection -> {
          JdbcAdmin.execute(connection, sqls, requiresExplicitCommit);
        });
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    String fullTableName = rdbEngine.encloseFullTableName(namespace, table);
    String sql = rdbEngine.internalTableExistsCheckSql(fullTableName);
    try {
      execute(sql);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw new Exception(
          String.format(
              "Checking if the %s table exists failed", getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws Exception {
    String dropTableStatement = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
    execute(dropTableStatement);
  }

  @Override
  public void dropNamespace(String namespace) throws SQLException {
    execute(rdbEngine.dropNamespaceSql(namespace));
  }

  @Override
  public boolean namespaceExists(String namespace) throws SQLException {
    String sql;
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?";
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      sql = "SELECT 1 FROM all_users WHERE username = ?";
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      sql = "SELECT 1 FROM pg_namespace WHERE nspname = ?";
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      // SQLite has no concept of namespace
      return true;
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      sql = "SELECT 1 FROM sys.schemas WHERE name = ?";
    } else if (JdbcTestUtils.isDb2(rdbEngine)) {
      sql = "SELECT 1 FROM syscat.schemata WHERE schemaname = ?";
    } else {
      throw new AssertionError("Unsupported engine : " + rdbEngine.getClass().getSimpleName());
    }

    return withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableFunction<Connection, Boolean, SQLException>)
            connection ->
                executeQuery(
                    connection,
                    sql,
                    requiresExplicitCommit,
                    ps -> ps.setString(1, namespace),
                    ResultSet::next));
  }

  /**
   * Renames a table at the storage level.
   *
   * @param namespace the namespace
   * @param oldTable the current table name
   * @param newTable the new table name
   * @throws SQLException if a database error occurs
   */
  public void renameTable(String namespace, String oldTable, String newTable) throws SQLException {
    execute(rdbEngine.renameTableSql(namespace, oldTable, newTable));
  }

  /**
   * Adds a column to a table at the storage level.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param column the column name to add
   * @param sqlType the SQL data type for the column (e.g., "INT", "BIGINT")
   * @throws SQLException if a database error occurs
   */
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void addColumn(String namespace, String table, String column, String sqlType)
      throws SQLException {
    String sql =
        "ALTER TABLE "
            + rdbEngine.encloseFullTableName(namespace, table)
            + " ADD "
            + rdbEngine.enclose(column)
            + " "
            + sqlType;
    execute(sql);
  }

  /**
   * Drops a column from a table at the storage level.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param column the column name to drop
   * @throws SQLException if a database error occurs
   */
  public void dropColumn(String namespace, String table, String column) throws SQLException {
    execute(rdbEngine.dropColumnSql(namespace, table, column));
  }

  /**
   * Renames a column at the storage level.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param oldColumn the current column name
   * @param newColumn the new column name
   * @param sqlType the SQL data type (required by some engines)
   * @throws SQLException if a database error occurs
   */
  public void renameColumn(
      String namespace, String table, String oldColumn, String newColumn, String sqlType)
      throws SQLException {
    execute(rdbEngine.renameColumnSql(namespace, table, oldColumn, newColumn, sqlType));
  }

  /**
   * Alters a column type at the storage level.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param column the column name
   * @param sqlType the new SQL data type
   * @throws SQLException if a database error occurs
   */
  public void alterColumnType(String namespace, String table, String column, String sqlType)
      throws SQLException {
    execute(rdbEngine.alterColumnTypeSql(namespace, table, column, sqlType));
  }

  /**
   * Renames an index at the storage level by computing old and new index names.
   *
   * @param namespace the namespace
   * @param oldTable the old table name (used to compute the old index name)
   * @param oldColumn the old column name (used to compute the old index name)
   * @param newTable the new table name (used to compute the new index name)
   * @param newColumn the new column name (used to compute the new index name)
   * @throws SQLException if a database error occurs
   */
  public void renameIndex(
      String namespace, String oldTable, String oldColumn, String newTable, String newColumn)
      throws SQLException {
    String oldIndexName = JdbcAdmin.getIndexName(namespace, oldTable, oldColumn);
    String newIndexName = JdbcAdmin.getIndexName(namespace, newTable, newColumn);
    execute(rdbEngine.renameIndexSqls(namespace, newTable, newColumn, oldIndexName, newIndexName));
  }

  /**
   * Checks if a column exists in a table using JDBC DatabaseMetaData.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param column the column name
   * @return true if the column exists
   * @throws SQLException if a database error occurs
   */
  public boolean columnExists(String namespace, String table, String column) throws SQLException {
    return withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableFunction<Connection, Boolean, SQLException>)
            connection -> {
              DatabaseMetaData meta = connection.getMetaData();
              String catalog = null;
              String schema = namespace;
              String tbl = table;
              String col = column;
              if (JdbcTestUtils.isOracle(rdbEngine)) {
                schema = namespace.toUpperCase();
                tbl = table.toUpperCase();
                col = column.toUpperCase();
              } else if (JdbcTestUtils.isMysql(rdbEngine)) {
                catalog = namespace;
                schema = null;
              }
              try (ResultSet rs = meta.getColumns(catalog, schema, tbl, col)) {
                return rs.next();
              }
            });
  }

  /**
   * Checks if an index with the specified name exists on a table using JDBC DatabaseMetaData.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param indexName the index name to check
   * @return true if the index exists
   * @throws SQLException if a database error occurs
   */
  public boolean indexExists(String namespace, String table, String indexName) throws SQLException {
    return withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableFunction<Connection, Boolean, SQLException>)
            connection -> {
              DatabaseMetaData meta = connection.getMetaData();
              String catalog = null;
              String schema = namespace;
              String tbl = table;
              if (JdbcTestUtils.isOracle(rdbEngine)) {
                schema = namespace.toUpperCase();
                tbl = table.toUpperCase();
              } else if (JdbcTestUtils.isMysql(rdbEngine)) {
                catalog = namespace;
                schema = null;
              }
              try (ResultSet rs = meta.getIndexInfo(catalog, schema, tbl, false, false)) {
                while (rs.next()) {
                  String name = rs.getString("INDEX_NAME");
                  if (indexName.equals(name)) {
                    return true;
                  }
                }
                return false;
              }
            });
  }

  /**
   * Returns the index name that ScalarDB would use for a given table and column.
   *
   * @param namespace the namespace
   * @param table the table name
   * @param column the indexed column name
   * @return the index name
   */
  public static String getIndexName(String namespace, String table, String column) {
    return JdbcAdmin.getIndexName(namespace, table, column);
  }

  /**
   * Deletes namespace metadata entry from the namespaces table.
   *
   * @param namespace the namespace to delete metadata for
   * @throws SQLException if a database error occurs
   */
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void deleteNamespaceMetadata(String namespace) throws SQLException {
    String sql =
        "DELETE FROM "
            + rdbEngine.encloseFullTableName(metadataSchema, NamespaceMetadataService.TABLE_NAME)
            + " WHERE "
            + rdbEngine.enclose("namespace_name")
            + " = '"
            + namespace
            + "'";
    execute(sql);
  }

  @Override
  public void close() throws SQLException {
    dataSource.close();
  }
}
