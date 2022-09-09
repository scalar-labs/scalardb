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

  @Disabled("Temporarily until admin.upgrade() is implemented")
  @Test
  @Override
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}
}
