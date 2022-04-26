package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import org.junit.jupiter.api.Disabled;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scanAll_NoLimitGiven_ShouldRetrieveAllRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scanAll_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scanAll_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scanAll_ScanAllWithLargeData_ShouldRetrieveExpectedValues() {}
}
