package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminRepairTableIntegrationTestWithCassandra
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTableAndCoordinatorTable_ForDeletedMetadataTable_ShouldRepairProperly() {}

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTableAndCoordinatorTable_ForTruncatedMetadataTable_ShouldRepairProperly() {}

  @Test
  @Disabled("there is no metadata table for Cassandra")
  @Override
  public void repairTable_ForCorruptedMetadataTable_ShouldRepairProperly() {}

  @Test
  public void repairTableAndCoordinatorTable_ShouldDoNothing() throws ExecutionException {
    // Act
    assertThatCode(
            () -> {
              admin.repairTable(
                  getNamespace(), getTable(), getTableMetadata(), getCreationOptions());
              admin.repairCoordinatorTables(getCreationOptions());
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    assertThat(admin.coordinatorTablesExist()).isTrue();
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
