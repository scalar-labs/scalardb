package com.scalar.db.transaction.consensuscommit;

import java.util.HashMap;
import java.util.Map;

public abstract class ConsensusCommitWithMetadataDecouplingIntegrationTestBase
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cc_decoupling";
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    Map<String, String> options = new HashMap<>(super.getCreationOptions());
    options.put("transaction_metadata_decoupling", "true");
    return options;
  }
}
