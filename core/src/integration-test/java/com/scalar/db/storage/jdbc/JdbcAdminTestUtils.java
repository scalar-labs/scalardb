package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminTestUtils extends AdminTestUtils {

  private final JdbcConfig config;
  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;

  public JdbcAdminTestUtils(Properties properties) {
    super(properties);
    config = new JdbcConfig(new DatabaseConfig(properties));
    metadataSchema = config.getMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    rdbEngine = RdbEngineFactory.create(config);
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute(
        "DROP TABLE " + rdbEngine.encloseFullTableName(metadataSchema, JdbcAdmin.METADATA_TABLE));
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
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
        Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }
}
