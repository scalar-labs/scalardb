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
  public void insert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void upsert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void update_DefaultNamespaceGiven_ShouldWorkProperly() {}

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
  public void updateAndCommit_UpdateGivenForNonExisting_ShouldDoNothing() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void
      updateAndCommit_UpdateWithUpdateIfExistsGivenForNonExisting_ShouldThrowUnsatisfiedConditionException() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void updateAndCommit_UpdateGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void updateAndCommit_UpdateWithUpdateIfExistsGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void update_withUpdateIfWithVerifiedCondition_shouldUpdateProperly() {}

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

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_insert_InsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_insert_InsertGivenForExisting_ShouldThrowCrudConflictException() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_upsert_UpsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_upsert_UpsertGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_update_UpdateGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_update_UpdateGivenForNonExisting_ShouldDoNothing() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_insert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_upsert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("ScalarDB Server doesn't support insert(), upsert(), and update()")
  @Override
  @Test
  public void manager_update_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetWithMatchedConjunctionsGivenForCommittedRecord_ShouldReturnRecord() {}

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetWithUnmatchedConjunctionsGivenForCommittedRecord_ShouldReturnEmpty() {}

  @Disabled("ScalarDB Server doesn't support scan() with conjunctions")
  @Override
  @Test
  public void scan_ScanWithConjunctionsGivenForCommittedRecord_ShouldReturnRecords() {}

  @Disabled("ScalarDB Server doesn't support scan() with conjunctions")
  @Override
  @Test
  public void scan_ScanGivenForIndexColumnWithConjunctions_ShouldReturnRecords() {}
}
