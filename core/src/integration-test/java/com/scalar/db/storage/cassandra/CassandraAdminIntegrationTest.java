package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    return CassandraEnv.getProperties();
  }

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled("Temporarily until admin.getNamespacesNames() is implemented")
  @Test
  @Override
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() {}
}
