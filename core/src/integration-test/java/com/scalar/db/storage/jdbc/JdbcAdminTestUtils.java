package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.executeQuery;
import static com.scalar.db.storage.jdbc.JdbcAdmin.withConnection;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;
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
            + " = "
            + getFullTableName(namespace, table);
    execute(deleteMetadataStatement);
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

  @Override
  public void close() throws SQLException {
    dataSource.close();
  }
}
