package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithJdbcDatabase
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }

  @Test
  @Override
  @DisabledIf("isSpanner")
  public void
      scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord()
          throws TransactionException {
    super
        .scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord();
  }

  @SuppressWarnings("unused")
  private boolean isSpanner() {
    return JdbcEnv.isSpanner();
  }
}
