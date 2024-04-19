package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final AutoCloseableDataSource dataSource;

  public JdbcAdminTestUtils(Properties properties) {
    super(properties);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    metadataSchema = config.getMetadataSchema();
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    execute(
        "DROP TABLE " + rdbEngine.encloseFullTableName(metadataSchema, JdbcAdmin.NAMESPACES_TABLE));
  }

  @Override
  public void dropMetadataTable() throws Exception {
    dropTable(metadataSchema, JdbcAdmin.METADATA_TABLE);
  }

  @Override
  public void truncateNamespacesTable() throws Exception {
    String truncateTableStatement =
        rdbEngine.truncateTableSql(metadataSchema, JdbcAdmin.NAMESPACES_TABLE);
    execute(truncateTableStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    String truncateTableStatement =
        rdbEngine.truncateTableSql(metadataSchema, JdbcAdmin.METADATA_TABLE);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + rdbEngine.encloseFullTableName(metadataSchema, JdbcAdmin.METADATA_TABLE)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    String fullTableName = rdbEngine.encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
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
    if (rdbEngine instanceof RdbEngineMysql) {
      sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?";
    } else if (rdbEngine instanceof RdbEngineOracle) {
      sql = "SELECT 1 FROM all_users WHERE username = ?";
    } else if (rdbEngine instanceof RdbEnginePostgresql) {
      sql = "SELECT 1 FROM pg_namespace WHERE nspname = ?";
    } else if (rdbEngine instanceof RdbEngineSqlite) {
      // SQLite has no concept of namespace
      return true;
    } else if (rdbEngine instanceof RdbEngineSqlServer) {
      sql = "SELECT 1 FROM sys.schemas WHERE name = ?";
    } else {
      throw new AssertionError("Unsupported engine : " + rdbEngine.getClass().getSimpleName());
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setString(1, namespace);
      ResultSet resultSet = preparedStatement.executeQuery();

      return resultSet.next();
    }
  }

  @Override
  public void close() throws Exception {
    dataSource.close();
  }
}
