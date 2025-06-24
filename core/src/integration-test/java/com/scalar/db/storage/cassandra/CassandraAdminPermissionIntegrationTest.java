package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return CassandraEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new CassandraPermissionTestUtils(getProperties(testName));
  }

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void getImportTableMetadata_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void addRawColumnToTable_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void importTable_WithSufficientPermission_ShouldSucceed() {}
}
