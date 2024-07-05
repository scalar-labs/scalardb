package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithDynamo
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitDynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new DynamoConfig(new DatabaseConfig(properties))
        .getTableMetadataNamespace()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected String getCoordinatorNamespaceName(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProps(testName)))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Override
  protected boolean isGroupCommitEnabled(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProps(testName)))
        .isCoordinatorGroupCommitEnabled();
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
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void namespaceExists_ShouldReturnCorrectResults() {}

  @Disabled
  @Test
  @Override
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}
}
