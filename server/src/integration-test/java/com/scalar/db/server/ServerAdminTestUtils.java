package com.scalar.db.server;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class ServerAdminTestUtils extends AdminTestUtils {

  private final RdbEngineStrategy rdbEngine;
  private final JdbcConfig config;
  private final String metadataNamespace;
  private final String metadataTable;

  public ServerAdminTestUtils(Properties jdbcStorageProperties) {
    super(jdbcStorageProperties);
    config = new JdbcConfig(new DatabaseConfig(jdbcStorageProperties));
    metadataNamespace = config.getMetadataSchema();
    metadataTable = JdbcAdmin.METADATA_TABLE;
    rdbEngine = RdbEngineFactory.create(config);
  }

  @Override
  public void dropNamespacesTable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropMetadataTable() throws Exception {
    dropTable(metadataNamespace, metadataTable);
  }

  @Override
  public void truncateNamespacesTable() {
    throw new UnsupportedOperationException();
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

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    String fullTableName = rdbEngine.encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
        Connection connection = dataSource.getConnection();
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
  public boolean namespaceExists(String namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropNamespace(String namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // Do nothing
  }
}
