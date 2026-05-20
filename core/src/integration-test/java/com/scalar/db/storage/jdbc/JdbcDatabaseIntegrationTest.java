package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;
  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace(), getTableName());
      return;
    }
    super.truncateTable();
  }

  @Override
  protected int getLargeDataSizeInBytes() {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      // For Oracle, the max data size for BLOB is 2000 bytes
      return 2000;
    } else {
      return super.getLargeDataSizeInBytes();
    }
  }
}
