package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.HashMap;
import java.util.Map;

public abstract class ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase
    extends ConsensusCommitImportTableIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "cc_import_dcpl";
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    Map<String, String> options = new HashMap<>(super.getCreationOptions());
    options.put("transaction-metadata-decoupling", "true");
    return options;
  }

  @Override
  protected String getImportedTableName() {
    return TABLE + "_scalardb";
  }

  @Override
  protected void assertTransactionMetadataNotStored(DistributedStorage storage, int id)
      throws ExecutionException {
    // With transaction metadata decoupling, the transaction metadata is stored in a separate table
    assertThat(
            storage.get(
                Get.newBuilder()
                    .namespace(namespace)
                    .table(TABLE + "_tx_metadata")
                    .partitionKey(createPartitionKey(id))
                    .build()))
        .isEmpty();
  }
}
