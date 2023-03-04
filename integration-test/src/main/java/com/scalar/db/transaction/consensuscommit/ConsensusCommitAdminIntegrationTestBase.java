package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.TransactionFactory;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public abstract class ConsensusCommitAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {
  private DistributedTransactionAdmin adminWithIncludeMetadataEnabled;

  @Override
  protected void initialize(String testName) throws Exception {
    Properties includeMetadataEnabledProperties = getPropsWithIncludeMetadataEnabled(testName);
    adminWithIncludeMetadataEnabled =
        TransactionFactory.create(includeMetadataEnabledProperties).getTransactionAdmin();
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();

    adminWithIncludeMetadataEnabled.close();
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    if (!properties.containsKey(DatabaseConfig.TRANSACTION_MANAGER)) {
      properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);
    }
    return properties;
  }

  protected abstract Properties getProps(String testName);

  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
    return properties;
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
