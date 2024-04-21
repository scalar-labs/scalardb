package com.scalar.db.server;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitIntegrationTestWithServer
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  private ScalarDbServer server1;
  private ScalarDbServer server2;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties1 = ServerEnv.getServer1Properties(testName);
    Properties properties2 = ServerEnv.getServer2Properties(testName);
    if (properties1 != null && properties2 != null) {
      server1 = new ScalarDbServer(modifyProperties(properties1, testName));
      server1.start();

      server2 = new ScalarDbServer(modifyProperties(properties2, testName));
      server2.start();
    }
  }

  private Properties modifyProperties(Properties properties, String testName) {
    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    return properties;
  }

  @Override
  protected Properties getProps1(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @Override
  protected Properties getProps2(String testName) {
    return ServerEnv.getClient2Properties(testName);
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server1 != null) {
      server1.shutdown();
    }
    if (server2 != null) {
      server2.shutdown();
    }
  }

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void insertAndCommit_InsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void
      insertAndCommit_InsertGivenForExisting_ShouldThrowCrudConflictExceptionOrPreparationConflictException() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void upsertAndCommit_UpsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void upsertAndCommit_UpsertGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void updateAndCommit_UpdateGivenForNonExisting_ShouldThrowRecordNotFoundException() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void updateAndCommit_UpsertGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void update_withUpdateIfWithVerifiedCondition_shouldPutProperly() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void
      update_withUpdateIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void
      update_withUpdateIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException() {}
}
