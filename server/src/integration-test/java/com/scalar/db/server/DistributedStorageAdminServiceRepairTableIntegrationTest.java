package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class DistributedStorageAdminServiceRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  private ScalarDbServer server;
  private boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
    } else {
      isExternalServerUsed = true;
    }
  }

  @Override
  protected Properties getProperties(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties == null) {
      return null;
    }

    return new ServerAdminTestUtils(properties);
  }

  @Override
  @AfterAll
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void repairTable_ForDeletedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForDeletedMetadataTable_ShouldRepairProperly();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void repairTable_ForTruncatedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForTruncatedMetadataTable_ShouldRepairProperly();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void repairTable_ForCorruptedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForCorruptedMetadataTable_ShouldRepairProperly();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerUsed() {
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    return isExternalServerUsed;
  }
}
