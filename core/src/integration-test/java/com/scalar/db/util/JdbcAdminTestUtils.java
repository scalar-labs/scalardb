package com.scalar.db.util;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final RdbEngine rdbEngine;
  private final JdbcConfig config;

  public JdbcAdminTestUtils(Properties properties) {
    super();
    config = new JdbcConfig(new DatabaseConfig(properties));
    metadataNamespace = config.getTableMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    metadataTable = JdbcAdmin.METADATA_TABLE;
    rdbEngine = JdbcUtils.getRdbEngine(config.getJdbcUrl());
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute(
        "DROP TABLE "
            + QueryUtils.enclosedFullTableName(metadataNamespace, metadataTable, rdbEngine));

    String dropNamespaceStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropNamespaceStatement = "DROP USER " + QueryUtils.enclose(metadataNamespace, rdbEngine);
    } else {
      dropNamespaceStatement = "DROP SCHEMA " + QueryUtils.enclose(metadataNamespace, rdbEngine);
    }
    execute(dropNamespaceStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + QueryUtils.enclosedFullTableName(metadataNamespace, metadataTable, rdbEngine);
    execute(truncateTableStatement);
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
