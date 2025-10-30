package com.scalar.db.server;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineMysql;
import com.scalar.db.storage.jdbc.RdbEngineOracle;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class ServerAdminTestUtils extends AdminTestUtils {

  private final RdbEngineStrategy rdbEngine;
  private final String metadataNamespace;
  private final String metadataTable;
  private final BasicDataSource dataSource;

  public ServerAdminTestUtils(Properties jdbcStorageProperties) {
    super(jdbcStorageProperties);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(jdbcStorageProperties));
    metadataNamespace =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    metadataTable = JdbcAdmin.METADATA_TABLE;
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute("DROP TABLE " + rdbEngine.encloseFullTableName(metadataNamespace, metadataTable));

    String dropNamespaceStatement;
    if (rdbEngine instanceof RdbEngineOracle) {
      dropNamespaceStatement = "DROP USER " + rdbEngine.enclose(metadataNamespace);
    } else {
      dropNamespaceStatement = "DROP SCHEMA " + rdbEngine.enclose(metadataNamespace);
    }
    execute(dropNamespaceStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    String truncateTableStatement =
        "TRUNCATE TABLE " + rdbEngine.encloseFullTableName(metadataNamespace, metadataTable);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + rdbEngine.encloseFullTableName(metadataNamespace, metadataTable)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  @Override
  public void deleteMetadata(String namespace, String table) throws Exception {
    String deleteMetadataStatement =
        "DELETE FROM "
            + rdbEngine.encloseFullTableName(metadataNamespace, JdbcAdmin.METADATA_TABLE)
            + " WHERE "
            + rdbEngine.enclose(JdbcAdmin.METADATA_COL_FULL_TABLE_NAME)
            + " = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(deleteMetadataStatement)) {
      preparedStatement.setString(1, getFullTableName(namespace, table));
      preparedStatement.executeUpdate();
    }
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws SQLException {
    String sql;
    if (rdbEngine instanceof RdbEngineMysql) {
      sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?";
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
  public void close() throws Exception {}
}
