package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdminImportTableIntegrationTestBase;
import java.util.Properties;

public abstract class ConsensusCommitAdminImportTableIntegrationTestBase
    extends DistributedTransactionAdminImportTableIntegrationTestBase {

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);
}
