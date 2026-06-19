package com.scalar.db.transaction.consensuscommit.cbrl;

/** {@link CbrlBackupRestoreIntegrationTest} with coordinator group commit enabled. */
public class CbrlBackupRestoreWithGroupCommitIntegrationTest
    extends CbrlBackupRestoreIntegrationTest {
  @Override
  protected boolean withCoordinatorGroupCommit() {
    return true;
  }
}
