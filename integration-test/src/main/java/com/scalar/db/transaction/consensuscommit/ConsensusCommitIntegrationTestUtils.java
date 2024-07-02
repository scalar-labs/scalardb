package com.scalar.db.transaction.consensuscommit;

import java.util.Properties;

public final class ConsensusCommitIntegrationTestUtils {

  private ConsensusCommitIntegrationTestUtils() {}

  public static void addSuffixToCoordinatorNamespace(Properties properties, String suffix) {
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + suffix);
  }

  public static String getCoordinatorNamespace(Properties properties) {
    return properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE);
  }
}
