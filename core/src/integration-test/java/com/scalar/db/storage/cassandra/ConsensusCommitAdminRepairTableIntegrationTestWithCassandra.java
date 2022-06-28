package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminRepairTableIntegrationTestWithCassandra
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps() {
    return CassandraEnv.getProperties();
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
  public void repairTableAndCoordinatorTable_ShouldDoNothing() throws ExecutionException {
    // Act
    assertThatCode(
            () -> {
              admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreateOptions());
              admin.repairCoordinatorTables(getCreateOptions());
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
    assertThat(admin.coordinatorTablesExist()).isTrue();
  }
}
