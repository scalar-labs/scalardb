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
    metadataSchema =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute(
        "DROP TABLE "
            + rdbEngine.encloseFullTableName(metadataSchema, TableMetadataService.TABLE_NAME));

    String dropNamespaceStatement = rdbEngine.dropNamespaceSql(metadataSchema);
    execute(dropNamespaceStatement);
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

  @Override
  public boolean namespaceExists(String namespace) throws SQLException {
    String sql;
    if (JdbcTestUtils.isMysql(rdbEngine) || JdbcTestUtils.isSpanner(rdbEngine)) {
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
    if (JdbcTestUtils.isSpanner(rdbEngine)) {
      dropAllIndexesForTable(namespace, table);
    }
    String dropTableStatement = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
    execute(dropTableStatement);
  }

  private void dropAllIndexesForTable(String namespace, String table) throws SQLException {
    // Spanner requires all indexes to be dropped before dropping a table.
    withConnection(
        dataSource,
        requiresExplicitCommit,
        connection -> {
          java.util.List<String> indexNames = new java.util.ArrayList<>();
          executeQuery(
              connection,
              "SELECT index_name FROM information_schema.indexes"
                  + " WHERE table_schema = ? AND table_name = ? AND index_type = 'INDEX'",
              requiresExplicitCommit,
              ps -> {
                ps.setString(1, namespace);
                ps.setString(2, table);
              },
              rs -> {
                while (rs.next()) {
                  indexNames.add(rs.getString(1));
                }
                return null;
              });
          for (String indexName : indexNames) {
            String dropIndexSql = rdbEngine.dropIndexSql(namespace, table, indexName);
            JdbcAdmin.execute(connection, dropIndexSql, requiresExplicitCommit);
          }
        });
  }

  @Override
  public void close() throws SQLException {
    dataSource.close();
  }
}
