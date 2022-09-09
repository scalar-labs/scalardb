package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithCassandra
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected Properties getProps() {
    return CassandraEnv.getProperties();
  }

  @Disabled("Temporarily until admin.upgrade() is implemented")
  @Test
  @Override
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}
}
