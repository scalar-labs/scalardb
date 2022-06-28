package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitAdminRepairTableIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps() {
    return JdbcEnv.getProperties();
  }
}
