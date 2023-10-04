package com.scalar.db.server;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitAdminRepairTableIntegrationTestWithServer
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  private ScalarDbServer server;
  boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

      server = new ScalarDbServer(properties);
      server.start();
    } else {
      isExternalServerUsed = true;
    }
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties == null) {
      return null;
    }

    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    return new ServerAdminTestUtils(properties);
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  /** These tests are disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @DisabledIf("isExternalServerUsed")
  public void repairTableAndCoordinatorTables_ForDeletedMetadataTable_ShouldRepairProperly()
      throws Exception {
    super.repairTableAndCoordinatorTables_ForDeletedMetadataTable_ShouldRepairProperly();
  }

  @Override
  @DisabledIf("isExternalServerUsed")
  public void repairTableAndCoordinatorTables_ForTruncatedMetadataTable_ShouldRepairProperly()
      throws Exception {
    super.repairTableAndCoordinatorTables_ForTruncatedMetadataTable_ShouldRepairProperly();
  }

  @Override
  @DisabledIf("isExternalServerUsed")
  public void repairTable_ForCorruptedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForCorruptedMetadataTable_ShouldRepairProperly();
  }

  @Override
  @DisabledIf("isExternalServerUsed")
  public void repairCoordinatorTables_CoordinatorTablesExist_ShouldDoNothing() throws Exception {
    super.repairCoordinatorTables_CoordinatorTablesExist_ShouldDoNothing();
  }

  @Override
  @DisabledIf("isExternalServerUsed")
  public void repairCoordinatorTables_CoordinatorTablesDoNotExist_ShouldCreateCoordinatorTables()
      throws Exception {
    super.repairCoordinatorTables_CoordinatorTablesDoNotExist_ShouldCreateCoordinatorTables();
  }

  @DisabledIf("isExternalServerUsed")
  @Override
  public void repairTable_ForNonExistingTableButExistingMetadata_ShouldCreateTable()
      throws Exception {
    super.repairTable_ForNonExistingTableButExistingMetadata_ShouldCreateTable();
  }

  @DisabledIf("isExternalServerUsed")
  @Override
  public void repairTable_ForExistingTableAndMetadata_ShouldDoNothing() throws Exception {
    super.repairTable_ForExistingTableAndMetadata_ShouldDoNothing();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerUsed() {
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    return isExternalServerUsed;
  }
}
