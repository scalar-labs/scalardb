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
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldThrowExecutionException() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowExecutionException() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() {}
}
