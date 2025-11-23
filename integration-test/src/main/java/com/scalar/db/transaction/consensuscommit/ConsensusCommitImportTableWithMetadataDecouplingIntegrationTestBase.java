package com.scalar.db.transaction.consensuscommit;

import java.util.HashMap;
import java.util.Map;

public abstract class ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase
    extends ConsensusCommitImportTableIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "cc_import_decoupling";
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    Map<String, String> options = new HashMap<>(super.getCreationOptions());
    options.put("transaction_metadata_decoupling", "true");
    return options;
  }

  @Override
  protected String getImportedTableName() {
    return TABLE + "_scalardb";
  }
}
