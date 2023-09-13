package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.AdminTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
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

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
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
                admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions()))
        .doesNotThrowAnyException();

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
  }
}
