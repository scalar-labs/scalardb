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
  protected DistributedTransactionAdmin adminWithIncludeMetadataEnabled;

  @BeforeAll
  @Override
  public void beforeAll() throws Exception {
    super.beforeAll();

    Properties includeMetadataEnabledProperties = TestUtils.addSuffix(getProperties(), TEST_NAME);
    includeMetadataEnabledProperties.setProperty(
        ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
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
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    return properties;
  }

  protected abstract Properties getProps();

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
