package com.scalar.db.storage.multistorage;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.query.QueryUtils;
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
  private final RdbEngine rdbEngine;

  public MultiStorageAdminTestUtils(Properties cassandraProperties, Properties jdbcProperties) {
    // Cassandra has the coordinator tables
    super(cassandraProperties);

    jdbcConfig = new JdbcConfig(new DatabaseConfig(jdbcProperties));
    jdbcMetadataSchema = jdbcConfig.getTableMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    rdbEngine = jdbcConfig.getRdbEngine();
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    // Do nothing for Cassandra

    // for JDBC
    execute(
        "DROP TABLE "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine));

    String dropNamespaceStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropNamespaceStatement = "DROP USER " + QueryUtils.enclose(jdbcMetadataSchema, rdbEngine);
    } else {
      dropNamespaceStatement = "DROP SCHEMA " + QueryUtils.enclose(jdbcMetadataSchema, rdbEngine);
    }
    execute(dropNamespaceStatement);
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
