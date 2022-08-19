package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DynamoAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties() {
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
  public void dropNamespace_ForNonExistingNamespace_ShouldExecutionException() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() {}
}
