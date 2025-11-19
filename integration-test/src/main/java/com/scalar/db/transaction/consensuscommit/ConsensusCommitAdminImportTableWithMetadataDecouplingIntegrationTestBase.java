package com.scalar.db.transaction.consensuscommit;

import java.util.HashMap;
import java.util.Map;

public abstract class ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestBase
    extends ConsensusCommitAdminImportTableIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cc_import_decoupling";
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    Map<String, String> options = new HashMap<>(super.getCreationOptions());
    options.put("transaction_metadata_decoupling", "true");
    return options;
  }

  @Override
  protected String getImportedTableName(String table) {
    return table + "_scalardb";
  }
}
