package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.executeQuery;
import static com.scalar.db.storage.jdbc.JdbcAdmin.withConnection;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.CoordinatorStateAccessor;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.ThrowableFunction;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final HikariDataSource dataSource;
  private final boolean requiresExplicitCommit;
  @Nullable private final String coordinatorNamespace;

  public JdbcAdminTestUtils(Properties properties) {
    super(properties);
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    JdbcConfig config = new JdbcConfig(databaseConfig);
    metadataSchema = config.getMetadataSchema();
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);

    // ConsensusCommitConfig requires scalar.db.transaction_manager to be 'consensus-commit', so
    // only resolve the coordinator namespace when applicable. For other transaction managers
    // (e.g., 'jdbc'), leave it null since they have no coordinator table.
    if (databaseConfig
        .getTransactionManager()
        .equals(ConsensusCommitConfig.TRANSACTION_MANAGER_NAME)) {
      coordinatorNamespace =
          new ConsensusCommitConfig(databaseConfig)
              .getCoordinatorNamespace()
              .orElse(CoordinatorStateAccessor.NAMESPACE);
    } else {
      coordinatorNamespace = null;
    }
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

  public void deleteAllRowsWithSql(String namespace, String table) throws ExecutionException {
    String sql = "DELETE FROM " + rdbEngine.encloseFullTableName(namespace, table);
    try {
      execute(sql);
    } catch (SQLException e) {
      throw new ExecutionException("Failed to delete all rows from " + namespace + "." + table, e);
    }
  }

  public void deleteAllRowsFromCoordinatorTableWithSql() throws ExecutionException {
    deleteAllRowsWithSql(coordinatorNamespace, CoordinatorStateAccessor.TABLE);
  }

  /**
   * Deletes all rows from the underlying source tables of a virtual table (view). With
   * metadata-decoupling, a table is a VIEW joining {@code <table>_data} and {@code
   * <table>_tx_metadata}. DELETE cannot target a multi-table view directly.
   */
  public void deleteAllRowsFromVirtualTableWithSql(String namespace, String table)
      throws ExecutionException {
    deleteAllRowsWithSql(namespace, table + "_data");
    deleteAllRowsWithSql(namespace, table + "_tx_metadata");
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void alterTableCollation(String namespace, String table, String collation)
      throws Exception {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      execute(
          "ALTER TABLE "
              + rdbEngine.encloseFullTableName(namespace, table)
              + " CONVERT TO CHARACTER SET utf8mb4 COLLATE "
              + collation);
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      alterTableCollationForSqlServer(namespace, table, collation);
    } else {
      throw new UnsupportedOperationException(
          "Altering the table collation is not supported for the "
              + rdbEngine.getClass().getSimpleName()
              + " engine");
    }
  }

  /**
   * Applies the collation to all character-typed columns of the table on SQL Server. SQL Server
   * rejects collation changes on columns that are part of a primary key or index, so the primary
   * key constraint is dropped first, each character column is altered with the collation (restating
   * its full data type and nullability retrieved from INFORMATION_SCHEMA), and the primary key
   * constraint is then re-added with its original column order and sort directions. The
   * drop/alter/re-add sequence runs in a single explicit transaction (SQL Server DDL is
   * transactional) so a midway failure rolls back to the original table definition instead of
   * leaving the table without its primary key.
   */
  private void alterTableCollationForSqlServer(String namespace, String table, String collation)
      throws SQLException {
    withConnection(
        dataSource,
        requiresExplicitCommit,
        connection -> {
          String fullTableName = rdbEngine.encloseFullTableName(namespace, table);

          // Retrieve the primary key constraint name
          String primaryKeyName =
              executeQuery(
                  connection,
                  "SELECT kc.name FROM sys.key_constraints kc"
                      + " JOIN sys.tables t ON kc.parent_object_id = t.object_id"
                      + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                      + " WHERE kc.type = 'PK' AND s.name = ? AND t.name = ?",
                  requiresExplicitCommit,
                  ps -> {
                    ps.setString(1, namespace);
                    ps.setString(2, table);
                  },
                  rs -> rs.next() ? rs.getString(1) : null);

          // Retrieve the primary key columns in their original order and sort directions
          List<String> primaryKeyColumnClauses = new ArrayList<>();
          executeQuery(
              connection,
              "SELECT c.name, ic.is_descending_key FROM sys.index_columns ic"
                  + " JOIN sys.indexes i"
                  + " ON ic.object_id = i.object_id AND ic.index_id = i.index_id"
                  + " JOIN sys.columns c"
                  + " ON ic.object_id = c.object_id AND ic.column_id = c.column_id"
                  + " JOIN sys.tables t ON i.object_id = t.object_id"
                  + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                  + " WHERE i.is_primary_key = 1 AND s.name = ? AND t.name = ?"
                  + " ORDER BY ic.key_ordinal",
              requiresExplicitCommit,
              ps -> {
                ps.setString(1, namespace);
                ps.setString(2, table);
              },
              rs -> {
                while (rs.next()) {
                  primaryKeyColumnClauses.add(
                      rdbEngine.enclose(rs.getString(1)) + (rs.getBoolean(2) ? " DESC" : " ASC"));
                }
                return null;
              });

          // Every ScalarDB-created SQL Server table has a primary key, so its absence proves a
          // prior broken run left the table without one. Fail fast instead of silently altering
          // the collation without dropping and re-adding the primary key.
          if (primaryKeyName == null || primaryKeyColumnClauses.isEmpty()) {
            throw new IllegalStateException(
                "No primary key found on "
                    + fullTableName
                    + "; a prior run may have failed after dropping the primary key");
          }

          // Build an ALTER COLUMN statement for each character-typed column, restating its full
          // data type, length, and nullability
          List<String> alterColumnStatements = new ArrayList<>();
          executeQuery(
              connection,
              "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE"
                  + " FROM INFORMATION_SCHEMA.COLUMNS"
                  + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                  + " AND DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar')",
              requiresExplicitCommit,
              ps -> {
                ps.setString(1, namespace);
                ps.setString(2, table);
              },
              rs -> {
                while (rs.next()) {
                  String columnName = rs.getString(1);
                  String dataType = rs.getString(2);
                  int maxLength = rs.getInt(3);
                  String length = maxLength == -1 ? "MAX" : String.valueOf(maxLength);
                  String nullability =
                      "YES".equalsIgnoreCase(rs.getString(4)) ? "NULL" : "NOT NULL";
                  alterColumnStatements.add(
                      "ALTER TABLE "
                          + fullTableName
                          + " ALTER COLUMN "
                          + rdbEngine.enclose(columnName)
                          + " "
                          + dataType
                          + "("
                          + length
                          + ") COLLATE "
                          + collation
                          + " "
                          + nullability);
                }
                return null;
              });

          // Run the drop/alter/re-add sequence atomically: SQL Server DDL is transactional, so a
          // midway failure rolls back to the original table definition, including the primary key
          boolean originalAutoCommit = connection.getAutoCommit();
          connection.setAutoCommit(false);
          try {
            // Pass false as requiresExplicitCommit so that each statement does not commit on its
            // own; the whole sequence is committed once below
            JdbcAdmin.execute(
                connection,
                "ALTER TABLE "
                    + fullTableName
                    + " DROP CONSTRAINT "
                    + rdbEngine.enclose(primaryKeyName),
                false);
            for (String alterColumnStatement : alterColumnStatements) {
              JdbcAdmin.execute(connection, alterColumnStatement, false);
            }
            JdbcAdmin.execute(
                connection,
                "ALTER TABLE "
                    + fullTableName
                    + " ADD CONSTRAINT "
                    + rdbEngine.enclose(primaryKeyName)
                    + " PRIMARY KEY ("
                    + String.join(",", primaryKeyColumnClauses)
                    + ")",
                false);
            connection.commit();
          } catch (SQLException e) {
            try {
              connection.rollback();
            } catch (SQLException rollbackEx) {
              e.addSuppressed(rollbackEx);
            }
            throw e;
          } finally {
            connection.setAutoCommit(originalAutoCommit);
          }
        });
  }

  @Override
  public int countRows(String namespace, String table) throws Exception {
    String sql = "SELECT COUNT(*) FROM " + rdbEngine.encloseFullTableName(namespace, table);
    return withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableFunction<Connection, Integer, SQLException>)
            connection ->
                executeQuery(
                    connection,
                    sql,
                    requiresExplicitCommit,
                    rs -> {
                      rs.next();
                      return rs.getInt(1);
                    }));
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
  public boolean tableExists(String namespace, String table) throws Exception {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          (ThrowableFunction<Connection, Boolean, SQLException>)
              connection ->
                  JdbcAdmin.internalTableExists(
                      connection, rdbEngine, namespace, table, requiresExplicitCommit));
    } catch (SQLException e) {
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
  public void dropNamespace(String namespace) throws SQLException {
    execute(rdbEngine.dropNamespaceSql(namespace));
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
  public void close() throws SQLException {
    dataSource.close();
  }
}
