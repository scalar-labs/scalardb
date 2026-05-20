package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  // Override setUp() to share the ClusterManager between admin and adminTestUtils so that schema
  // changes made via one are visible to the other. Without sharing, schema metadata propagates
  // asynchronously across driver sessions and the repair-then-tableExists test sequence becomes
  // flaky.
  @Override
  @BeforeEach
  protected void setUp() throws Exception {
    Properties properties = getProperties(TEST_NAME);
    ClusterManager clusterManager = new ClusterManager(new DatabaseConfig(properties));
    admin = new CassandraAdmin(clusterManager, new DatabaseConfig(properties));
    adminTestUtils = new CassandraAdminTestUtils(properties, clusterManager);
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(getNamespace(), options);
    admin.createTable(getNamespace(), getTable(), getTableMetadata(), options);
  }

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTable_ForDeletedMetadataTable_ShouldRepairProperly() {}

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTable_ForTruncatedMetadataTable_ShouldRepairProperly() {}

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTable_ForCorruptedMetadataTable_ShouldRepairProperly() {}

  @Test
  public void repairTable_ShouldDoNothing() throws ExecutionException {
    // Act
    assertThatCode(
            () ->
                admin.repairTable(
                    getNamespace(), getTable(), getTableMetadata(), getCreationOptions()))
        .doesNotThrowAnyException();

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
