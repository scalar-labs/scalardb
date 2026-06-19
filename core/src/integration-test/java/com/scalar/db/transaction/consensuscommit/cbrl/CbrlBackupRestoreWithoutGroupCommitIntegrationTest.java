package com.scalar.db.transaction.consensuscommit.cbrl;

/** {@link CbrlBackupRestoreIntegrationTest} with coordinator group commit disabled. */
public class CbrlBackupRestoreWithoutGroupCommitIntegrationTest
    extends CbrlBackupRestoreIntegrationTest {
  @Override
  protected boolean withCoordinatorGroupCommit() {
    return false;
  }
}
