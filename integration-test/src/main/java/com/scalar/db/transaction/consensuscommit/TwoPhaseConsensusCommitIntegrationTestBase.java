package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.TwoPhaseCommitTransactionIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class TwoPhaseConsensusCommitIntegrationTestBase
    extends TwoPhaseCommitTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_2pcc";
  }

  @Override
  protected final Properties getProperties1(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps1(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  @Override
  protected final Properties getProperties2(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps2(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps1(String testName);

  protected Properties getProps2(String testName) {
    return getProps1(testName);
  }

  @Disabled("Implement later")
  @Override
  @Test
  public void getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords() {}

  @Disabled("Implement later")
  @Override
  @Test
  public void manager_getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords() {}
}
