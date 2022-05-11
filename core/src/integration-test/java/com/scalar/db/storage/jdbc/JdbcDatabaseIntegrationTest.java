package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues() {}

  @Override
  @Disabled("ScanAll is not yet implemented for JDBC databases")
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues() {}
}
