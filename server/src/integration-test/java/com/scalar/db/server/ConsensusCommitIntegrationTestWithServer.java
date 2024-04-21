package com.scalar.db.server;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitIntegrationTestWithServer extends ConsensusCommitIntegrationTestBase {

  private ScalarDbServer server;

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
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
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
      insertAndCommit_InsertGivenForExisting_ShouldThrowCrudConflictExceptionOrCommitConflictException() {}

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
