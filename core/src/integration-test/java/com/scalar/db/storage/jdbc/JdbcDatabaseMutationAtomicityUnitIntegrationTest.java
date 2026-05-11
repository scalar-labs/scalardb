package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Properties;

public class JdbcDatabaseMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      JdbcTestUtils.deleteAllRowsWithSql(rdbEngine, getNamespace1(), TABLE1);
      JdbcTestUtils.deleteAllRowsWithSql(rdbEngine, getNamespace1(), TABLE2);
      JdbcTestUtils.deleteAllRowsWithSql(rdbEngine, getNamespace2(), TABLE1);
      JdbcTestUtils.deleteAllRowsWithSql(rdbEngine, getNamespace3(), TABLE1);
      return;
    }
    super.truncateTable();
  }
}
