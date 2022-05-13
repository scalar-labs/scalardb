package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithDynamo
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  // Since DynamoDB doesn't have the namespace concept, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors

  @Disabled
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldExecutionException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldExecutionException() {}
}
