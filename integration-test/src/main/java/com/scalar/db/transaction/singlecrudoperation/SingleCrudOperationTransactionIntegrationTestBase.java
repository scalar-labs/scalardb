package com.scalar.db.transaction.singlecrudoperation;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public abstract class SingleCrudOperationTransactionIntegrationTestBase
    extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_sco";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    properties.putIfAbsent(
        DatabaseConfig.TRANSACTION_MANAGER,
        SingleCrudOperationTransactionConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Override
  protected void populateRecords() throws TransactionException {
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        InsertBuilder.Buildable insert =
            Insert.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey);
        prepareNonKeyColumns(i, j).forEach(insert::value);
        manager.insert(insert.build());
      }
    }
  }

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetWithProjectionGivenForCommittedRecord_ShouldReturnRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetWithMatchedConjunctionsGivenForCommittedRecord_ShouldReturnRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetWithUnmatchedConjunctionsGivenForCommittedRecord_ShouldReturnEmpty() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanGivenForCommittedRecord_ShouldReturnRecords(ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanWithProjectionsGivenForCommittedRecord_ShouldReturnRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanWithLimitGivenForCommittedRecord_ShouldReturnRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanGivenForNonExisting_ShouldReturnEmpty(ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanGivenForIndexColumn_ShouldReturnRecords(ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanAllGivenForCommittedRecord_ShouldReturnRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanAllGivenWithLimit_ShouldReturnLimitedAmountOfRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanAllGivenForNonExisting_ShouldReturnEmpty(ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putAndCommit_PutGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putWithNullValueAndCommit_ShouldCreateRecordProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putAndAbort_ShouldNotCreateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void putAndRollback_ShouldNotCreateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void deleteAndCommit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void deleteAndCommit_DeleteGivenForExisting_ShouldDeleteRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void deleteAndAbort_ShouldNotDeleteRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void deleteAndRollback_ShouldNotDeleteRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void mutateAndCommit_AfterRead_ShouldMutateRecordsProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void mutateAndCommit_ShouldMutateRecordsProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      get_GetWithProjectionOnNonPrimaryKeyColumnsForGivenForCommittedRecord_ShouldReturnOnlyProjectedColumns() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void
      scanOrGetScanner_ScanWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns(
          ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void
      scanOrGetScanner_ScanAllWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns(
          ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support resuming a transaction")
  @Override
  @Test
  public void resume_WithBeginningTransaction_ShouldReturnBegunTransaction() {}

  @Disabled("Single CRUD operation transactions don't support resuming a transaction")
  @Override
  @Test
  public void resume_WithoutBeginningTransaction_ShouldThrowTransactionNotFoundException() {}

  @Disabled("Single CRUD operation transactions don't support resuming a transaction")
  @Override
  @Test
  public void
      resume_WithBeginningAndCommittingTransaction_ShouldThrowTransactionNotFoundException() {}

  @Disabled("Single CRUD operation transactions don't support resuming a transaction")
  @Override
  @Test
  public void
      resume_WithBeginningAndRollingBackTransaction_ShouldThrowTransactionNotFoundException() {}

  @Disabled("Single CRUD operation transactions don't support resuming a transaction")
  @Override
  @Test
  public void get_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void scan_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void getScanner_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void insert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void upsert_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void update_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void delete_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void mutate_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfWithVerifiedCondition_shouldPutProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfExistsWhenRecordExists_shouldPutProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfNotExistsWhenRecordDoesNotExist_shouldPutProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void delete_withDeleteIfWithVerifiedCondition_shouldDeleteProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void delete_withDeleteIfExistsWhenRecordsExists_shouldDeleteProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      put_withPutIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfNotExistsWhenRecordExists_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      delete_withDeleteIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      delete_withDeleteIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void put_withPutIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void insertAndCommit_InsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      insertAndCommit_InsertGivenForExisting_ShouldThrowCrudConflictExceptionOrCommitConflictException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void upsertAndCommit_UpsertGivenForNonExisting_ShouldCreateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void upsertAndCommit_UpsertGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void updateAndCommit_UpdateGivenForNonExisting_ShouldDoNothing() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      updateAndCommit_UpdateWithUpdateIfExistsGivenForNonExisting_ShouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void updateAndCommit_UpdateGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void updateAndCommit_UpdateWithUpdateIfExistsGivenForExisting_ShouldUpdateRecord() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void update_withUpdateIfWithVerifiedCondition_shouldUpdateProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      update_withUpdateIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @Test
  public void
      update_withUpdateIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException() {}

  @Disabled("Single CRUD operation transactions don't support getState()")
  @Override
  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("Single CRUD operation transactions don't support getState()")
  @Override
  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("Single CRUD operation transactions don't support abort()")
  @Override
  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("Single CRUD operation transactions don't support rollback()")
  @Override
  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}

  @Disabled(
      "Single CRUD operation transactions don't support executing multiple mutations in a transaction")
  @Override
  @Test
  public void manager_mutate_DefaultNamespaceGiven_ShouldWorkProperly() {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanWithConjunctionsGivenForCommittedRecord_ShouldReturnRecords(
      ScanType scanType) {}

  @Disabled("Single CRUD operation transactions don't support beginning a transaction")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanGivenForIndexColumnWithConjunctions_ShouldReturnRecords(
      ScanType scanType) {}
}
