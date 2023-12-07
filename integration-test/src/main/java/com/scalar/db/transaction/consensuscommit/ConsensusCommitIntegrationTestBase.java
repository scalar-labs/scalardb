package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import java.util.Properties;

public abstract class ConsensusCommitIntegrationTestBase
    extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cc";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);
}
