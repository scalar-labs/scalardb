package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionAdminRepairTableIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public abstract class ConsensusCommitAdminRepairTableIntegrationTestBase
    extends DistributedTransactionAdminRepairTableIntegrationTestBase {

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Test
  public void
      repairCoordinatorTables_WithExistingCoordinatorMissingChildIdsColumn_ShouldAddChildIdsColumn()
          throws Exception {
    // Arrange: drop the Coordinator created in setUp and recreate it via a separate admin
    // instance that has group commit explicitly disabled, so the Coordinator table has no
    // CHILD_IDS column.
    admin.dropCoordinatorTables();

    Properties propertiesWithGroupCommitDisabled = new Properties();
    propertiesWithGroupCommitDisabled.putAll(getProperties(TEST_NAME));
    propertiesWithGroupCommitDisabled.setProperty(
        ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED, "false");
    try (DistributedTransactionAdmin adminWithGroupCommitDisabled =
        TransactionFactory.create(propertiesWithGroupCommitDisabled).getTransactionAdmin()) {
      waitForDifferentSessionDdl();
      adminWithGroupCommitDisabled.createCoordinatorTables(getCreationOptions());
    }

    // Act: repair with group commit enabled. The existing Coordinator has no CHILD_IDS column,
    // so this should add the column via ALTER TABLE ADD COLUMN.
    Properties propertiesWithGroupCommitEnabled = new Properties();
    propertiesWithGroupCommitEnabled.putAll(getProperties(TEST_NAME));
    propertiesWithGroupCommitEnabled.setProperty(
        ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED, "true");
    try (DistributedTransactionAdmin adminWithGroupCommitEnabled =
        TransactionFactory.create(propertiesWithGroupCommitEnabled).getTransactionAdmin()) {
      waitForDifferentSessionDdl();
      adminWithGroupCommitEnabled.repairCoordinatorTables(getCreationOptions());

      // Assert: the column now exists in the post-repair Coordinator metadata. We have to read the
      // metadata via DistributedStorageAdmin (not via the transaction admin) because
      // ConsensusCommitAdmin#getTableMetadata returns null for the coordinator namespace by design.
      waitForDifferentSessionDdl();
      String coordinatorNamespace =
          new ConsensusCommitConfig(new DatabaseConfig(propertiesWithGroupCommitEnabled))
              .getCoordinatorNamespace()
              .orElse(Coordinator.NAMESPACE);
      try (DistributedStorageAdmin storageAdmin =
          StorageFactory.create(propertiesWithGroupCommitEnabled).getStorageAdmin()) {
        TableMetadata metadata =
            storageAdmin.getTableMetadata(coordinatorNamespace, Coordinator.TABLE);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getColumnNames()).contains(Attribute.CHILD_IDS);
      }
    }
  }

  @Test
  public void
      repairCoordinatorTables_WithExistingCoordinatorHavingChildIdsColumnAndGroupCommitDisabled_ShouldPreserveChildIdsColumn()
          throws Exception {
    // Arrange: drop the Coordinator created in setUp and recreate it via a separate admin
    // instance that has group commit explicitly enabled, so the Coordinator table has the
    // CHILD_IDS column.
    admin.dropCoordinatorTables();

    Properties propertiesWithGroupCommitEnabled = new Properties();
    propertiesWithGroupCommitEnabled.putAll(getProperties(TEST_NAME));
    propertiesWithGroupCommitEnabled.setProperty(
        ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED, "true");
    try (DistributedTransactionAdmin adminWithGroupCommitEnabled =
        TransactionFactory.create(propertiesWithGroupCommitEnabled).getTransactionAdmin()) {
      waitForDifferentSessionDdl();
      adminWithGroupCommitEnabled.createCoordinatorTables(getCreationOptions());
    }

    // Act: repair with group commit disabled. The existing Coordinator has the CHILD_IDS column,
    // so the repair should preserve it (keep the WITH-CHILD_IDS schema) rather than dropping it,
    // so that ScalarDB metadata stays aligned with the physical column set.
    Properties propertiesWithGroupCommitDisabled = new Properties();
    propertiesWithGroupCommitDisabled.putAll(getProperties(TEST_NAME));
    propertiesWithGroupCommitDisabled.setProperty(
        ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED, "false");
    try (DistributedTransactionAdmin adminWithGroupCommitDisabled =
        TransactionFactory.create(propertiesWithGroupCommitDisabled).getTransactionAdmin()) {
      waitForDifferentSessionDdl();
      adminWithGroupCommitDisabled.repairCoordinatorTables(getCreationOptions());

      // Assert: the column is still present in the post-repair Coordinator metadata. We have to
      // read the metadata via DistributedStorageAdmin (not via the transaction admin) because
      // ConsensusCommitAdmin#getTableMetadata returns null for the coordinator namespace by design.
      waitForDifferentSessionDdl();
      String coordinatorNamespace =
          new ConsensusCommitConfig(new DatabaseConfig(propertiesWithGroupCommitDisabled))
              .getCoordinatorNamespace()
              .orElse(Coordinator.NAMESPACE);
      try (DistributedStorageAdmin storageAdmin =
          StorageFactory.create(propertiesWithGroupCommitDisabled).getStorageAdmin()) {
        TableMetadata metadata =
            storageAdmin.getTableMetadata(coordinatorNamespace, Coordinator.TABLE);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getColumnNames()).contains(Attribute.CHILD_IDS);
      }
    }
  }
}
