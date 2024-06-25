package com.scalar.db.storage.multistorage;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineOracle;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class MultiStorageAdminTestUtils extends AdminTestUtils {

  private final JdbcConfig jdbcConfig;
  private final String jdbcMetadataSchema;
  private final RdbEngineStrategy rdbEngine;

  public MultiStorageAdminTestUtils(Properties cassandraProperties, Properties jdbcProperties) {
    // Cassandra has the coordinator tables
    super(cassandraProperties);

    jdbcConfig = new JdbcConfig(new DatabaseConfig(jdbcProperties));
    jdbcMetadataSchema =
        jdbcConfig.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    rdbEngine = RdbEngineFactory.create(jdbcConfig);
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    // Do nothing for Cassandra

    // for JDBC
    execute(
        "DROP TABLE "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE));

    String dropNamespaceStatement;
    if (rdbEngine instanceof RdbEngineOracle) {
      dropNamespaceStatement = "DROP USER " + rdbEngine.enclose(jdbcMetadataSchema);
    } else {
      dropNamespaceStatement = "DROP SCHEMA " + rdbEngine.enclose(jdbcMetadataSchema);
    }
    execute(dropNamespaceStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig, rdbEngine);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
