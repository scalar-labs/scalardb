package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class ConsensusCommitAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {
  private DistributedTransactionAdmin adminWithIncludeMetadataEnabled;

  @BeforeAll
  @Override
  public void beforeAll() throws Exception {
    super.beforeAll();

    Properties includeMetadataEnabledProperties =
        TestUtils.addSuffix(getPropsWithIncludeMetadataEnabled(), TEST_NAME);
    adminWithIncludeMetadataEnabled =
        TransactionFactory.create(includeMetadataEnabledProperties).getTransactionAdmin();
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();

    adminWithIncludeMetadataEnabled.close();
  }

  @Override
  protected final Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(getProps());
    String transactionManager = properties.getProperty(DatabaseConfig.TRANSACTION_MANAGER, "");
    if (!transactionManager.equals("grpc")) {
      properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

      // Async commit can cause unexpected lazy recoveries, which can fail the tests. So we disable
      // it for now.
      properties.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "false");
    }
    return properties;
  }

  protected abstract Properties getProps();

  protected Properties getPropsWithIncludeMetadataEnabled() {
    Properties properties = getProperties();
    properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
    return properties;
  }

  @Override
  protected String getCoordinatorNamespace() {
    return new ConsensusCommitConfig(new DatabaseConfig(getStorageProperties()))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Test
  public void
      getTableMetadata_WhenIncludeMetadataIsEnabled_ShouldReturnCorrectMetadataWithTransactionMetadataColumns()
          throws ExecutionException {
    // Arrange
    TableMetadata transactionTableMetadata =
        ConsensusCommitUtils.buildTransactionTableMetadata(TABLE_METADATA);

    // Act
    TableMetadata tableMetadata =
        adminWithIncludeMetadataEnabled.getTableMetadata(getNamespace1(), TABLE1);

    // Assert
    assertThat(tableMetadata).isEqualTo(transactionTableMetadata);
  }
}
