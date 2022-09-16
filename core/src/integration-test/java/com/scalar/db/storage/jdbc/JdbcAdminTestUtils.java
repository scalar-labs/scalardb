package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final JdbcConfig config;
  private final String metadataSchema;
  private final RdbEngine rdbEngine;

  public JdbcAdminTestUtils(Properties properties) {
    super(properties);
    config = new JdbcConfig(new DatabaseConfig(properties));
    metadataSchema = config.getMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    rdbEngine = JdbcUtils.getRdbEngine(config.getJdbcUrl());
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute(
        "DROP TABLE " + enclosedFullTableName(metadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine));
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + enclosedFullTableName(metadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + enclosedFullTableName(metadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    execute(
        "DROP TABLE "
            + enclosedFullTableName(metadataSchema, JdbcAdmin.NAMESPACES_TABLE, rdbEngine));
  }
}
