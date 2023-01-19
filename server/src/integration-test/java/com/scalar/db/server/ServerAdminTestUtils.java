package com.scalar.db.server;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class ServerAdminTestUtils extends AdminTestUtils {

  private final RdbEngine rdbEngine;
  private final JdbcConfig config;
  private final String metadataNamespace;
  private final String metadataTable;

  public ServerAdminTestUtils(Properties jdbcStorageProperties) {
    super(jdbcStorageProperties);
    config = new JdbcConfig(new DatabaseConfig(jdbcStorageProperties));
    metadataNamespace = config.getTableMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    metadataTable = JdbcAdmin.METADATA_TABLE;
    rdbEngine = RdbEngineStrategy.create(config).getRdbEngine();
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    execute("DROP TABLE " + enclosedFullTableName(metadataNamespace, metadataTable, rdbEngine));

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
        "TRUNCATE TABLE " + enclosedFullTableName(metadataNamespace, metadataTable, rdbEngine);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + enclosedFullTableName(metadataNamespace, metadataTable, rdbEngine)
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
}
