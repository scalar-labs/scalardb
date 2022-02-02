package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RecoveryHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_1 = 200;
  private static final long ANY_TIME_2 = 100;

  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private TransactionalTableMetadataManager tableMetadataManager;
  @Mock private ConsensusCommitConfig config;

  private RecoveryHandler handler;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    handler =
        spy(
            new RecoveryHandler(
                storage, coordinator, tableMetadataManager, new ParallelExecutor(config)));
    when(tableMetadataManager.getTransactionalTableMetadata(any()))
        .thenReturn(
            new TransactionalTableMetadata(
                ConsensusCommitUtils.buildTransactionalTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn(ANY_NAME_1, DataType.TEXT)
                        .addColumn(ANY_NAME_2, DataType.TEXT)
                        .addColumn(ANY_NAME_3, DataType.TEXT)
                        .addPartitionKey(ANY_NAME_1)
                        .addClusteringKey(ANY_NAME_2)
                        .build())));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_KEYSPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private TransactionResult prepareResult(
      long preparedAt, TransactionState state, boolean hasBeforeImageColumns) {
    Result result = mock(Result.class);
    ImmutableMap.Builder<String, Value<?>> builder =
        ImmutableMap.<String, Value<?>>builder()
            .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_3))
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_1))
            .put(Attribute.PREPARED_AT, Attribute.toPreparedAtValue(preparedAt))
            .put(Attribute.COMMITTED_AT, Attribute.toCommittedAtValue(0))
            .put(Attribute.STATE, Attribute.toStateValue(state))
            .put(Attribute.VERSION, Attribute.toVersionValue(2));
    if (hasBeforeImageColumns) {
      builder
          .put(
              Attribute.BEFORE_PREFIX + ANY_NAME_3,
              new TextValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_TEXT_4))
          .put(Attribute.BEFORE_ID, Attribute.toBeforeIdValue(ANY_ID_2))
          .put(Attribute.BEFORE_PREPARED_AT, Attribute.toBeforePreparedAtValue(ANY_TIME_2))
          .put(Attribute.BEFORE_COMMITTED_AT, Attribute.toBeforeCommittedAtValue(ANY_TIME_2))
          .put(Attribute.BEFORE_STATE, Attribute.toBeforeStateValue(TransactionState.COMMITTED))
          .put(Attribute.BEFORE_VERSION, Attribute.toBeforeVersionValue(1));
    } else {
      builder
          .put(
              Attribute.BEFORE_PREFIX + ANY_NAME_3,
              new TextValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, (String) null))
          .put(Attribute.BEFORE_ID, Attribute.toBeforeIdValue(null))
          .put(Attribute.BEFORE_PREPARED_AT, Attribute.toBeforePreparedAtValue(0))
          .put(Attribute.BEFORE_COMMITTED_AT, Attribute.toBeforeCommittedAtValue(0))
          .put(Attribute.BEFORE_STATE, new IntValue(Attribute.BEFORE_STATE, 0))
          .put(Attribute.BEFORE_VERSION, Attribute.toBeforeVersionValue(0));
    }
    when(result.getValues()).thenReturn(builder.build());
    when(result.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1))));
    when(result.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2))));
    return new TransactionResult(result);
  }

  @Test
  public void
      recover_GetAndResultGivenWhenStorageReturnsResultWithCommittedState_ShouldReturnResultReturnedByStorage()
          throws RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenStorageReturnsResultWithCommittedState_ShouldReturnResultReturnedByStorage(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultGivenWhenStorageReturnsResultWithCommittedState_ShouldReturnResultReturnedByStorage()
          throws RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenStorageReturnsResultWithCommittedState_ShouldReturnResultReturnedByStorage(
        prepareScan());
  }

  private void
      recover_SelectionAndResultGivenWhenStorageReturnsResultWithCommittedState_ShouldReturnResultReturnedByStorage(
          Selection selection) throws RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.PREPARED, true);

    TransactionResult expected = prepareResult(ANY_TIME_1, TransactionState.COMMITTED, true);
    when(storage.get(any())).thenReturn(Optional.of(expected));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler, never()).rollforwardRecord(any(), any());
    verify(handler, never()).rollbackRecord(any(), any());
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void recover_GetAndResultGivenWhenStorageReturnsEmpty_ShouldReturnEmpty()
      throws RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenStorageReturnsEmpty_ShouldReturnEmpty(prepareGet());
  }

  @Test
  public void recover_ScanAndResultGivenWhenStorageReturnsEmpty_ShouldReturnEmpty()
      throws RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenStorageReturnsEmpty_ShouldReturnEmpty(prepareScan());
  }

  private void recover_SelectionAndResultGivenWhenStorageReturnsEmpty_ShouldReturnEmpty(
      Selection selection) throws RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.PREPARED, true);
    when(storage.get(any())).thenReturn(Optional.empty());

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler, never()).rollforwardRecord(any(), any());
    verify(handler, never()).rollbackRecord(any(), any());
    assertThat(actual).isEqualTo(Optional.empty());
  }

  @Test
  public void
      recover_GetAndResultWithPreparedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnAfterImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithPreparedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnAfterImageResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultWithPreparedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnAfterImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithPreparedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnAfterImageResult(
        prepareScan());
  }

  private void
      recover_SelectionAndResultWithPreparedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnAfterImageResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.PREPARED, true);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED)));
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler).rollforwardRecord(selection, result);
    assertAfterImageResult(actual);
  }

  @Test
  public void
      recover_GetAndDeletedResultWithDeletedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndDeletedResultWithDeletedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnEmptyResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndDeletedResultWithDeletedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndDeletedResultWithDeletedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnEmptyResult(
        prepareScan());
  }

  private void
      recover_SelectionAndDeletedResultWithDeletedStateGivenWhenCoordinatorStateCommitted_ShouldRollforwardAndReturnEmptyResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.DELETED, true);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED)));
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler).rollforwardRecord(selection, result);
    assertThat(actual).isNotPresent();
  }

  @Test
  public void
      recover_GetAndResultGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnBeforeImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnBeforeImageResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnBeforeImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnBeforeImageResult(
        prepareScan());
  }

  private void
      recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnBeforeImageResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.PREPARED, true);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED)));
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler).rollbackRecord(selection, result);
    assertThat(actual).isPresent();
    assertBeforeImageResult(actual);
  }

  @Test
  public void
      recover_GetAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnEmptyResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnEmptyResult(
        prepareScan());
  }

  private void
      recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateAborted_ShouldRollbackAndReturnEmptyResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(ANY_TIME_1, TransactionState.PREPARED, false);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED)));
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(handler).rollbackRecord(selection, result);
    assertThat(actual).isNotPresent();
  }

  //  @Test
  //  public void
  //
  // recover_GetAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnBeforeImageResult()
  //          throws CoordinatorException, RecoveryException, ExecutionException {
  //
  // recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnBeforeImageResult(
  //        prepareGet());
  //  }
  //
  //  @Test
  //  public void
  //
  // recover_ScanAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnBeforeImageResult()
  //          throws CoordinatorException, RecoveryException, ExecutionException {
  //
  // recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnBeforeImageResult(
  //        prepareScan());
  //  }
  //
  //  private void
  //
  // recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnBeforeImageResult(
  //          Selection selection) throws CoordinatorException, RecoveryException,
  // ExecutionException {
  //    // Arrange
  //    TransactionResult result =
  //        prepareResult(System.currentTimeMillis(), TransactionState.PREPARED, true);
  //    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
  //    when(storage.get(any())).thenReturn(Optional.of(result));
  //    doNothing().when(handler).rollforwardRecord(any(Selection.class),
  // any(TransactionResult.class));
  //
  //    // Act
  //    Optional<TransactionResult> actual = handler.recover(selection, result);
  //
  //    // Assert
  //    verify(coordinator, never())
  //        .putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
  //    verify(handler, never()).rollbackRecord(selection, result);
  //    assertBeforeImageResult(actual);
  //  }

  //  @Test
  //  public void
  //
  // recover_GetAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnEmptyResult()
  //          throws CoordinatorException, RecoveryException, ExecutionException {
  //
  // recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnEmptyResult(
  //        prepareGet());
  //  }
  //
  //  @Test
  //  public void
  //
  // recover_ScanAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnEmptyResult()
  //          throws CoordinatorException, RecoveryException, ExecutionException {
  //
  // recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnEmptyResult(
  //        prepareScan());
  //  }
  //
  //  private void
  //
  // recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldNotRecoverRecordAndReturnEmptyResult(
  //          Selection selection) throws CoordinatorException, RecoveryException,
  // ExecutionException {
  //    // Arrange
  //    TransactionResult result =
  //        prepareResult(System.currentTimeMillis(), TransactionState.PREPARED, false);
  //    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
  //    when(storage.get(any())).thenReturn(Optional.of(result));
  //    doNothing().when(handler).rollforwardRecord(any(Selection.class),
  // any(TransactionResult.class));
  //
  //    // Act
  //    Optional<TransactionResult> actual = handler.recover(selection, result);
  //
  //    // Assert
  //    verify(coordinator, never())
  //        .putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
  //    verify(handler, never()).rollbackRecord(selection, result);
  //    assertThat(actual).isNotPresent();
  //  }

  @Test
  public void
      recover_GetAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnBeforeImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnBeforeImageResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnBeforeImageResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnBeforeImageResult(
        prepareScan());
  }

  private void
      recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnBeforeImageResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result =
        prepareResult(
            System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS * 2,
            TransactionState.PREPARED,
            true);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(handler).rollbackRecord(selection, result);
    assertBeforeImageResult(actual);
  }

  @Test
  public void
      recover_GetAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnEmptyResult(
        prepareGet());
  }

  @Test
  public void
      recover_ScanAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnEmptyResult()
          throws CoordinatorException, RecoveryException, ExecutionException {
    recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnEmptyResult(
        prepareScan());
  }

  private void
      recover_SelectionAndResultWithoutBeforeImageColumnsGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbortAndReturnEmptyResult(
          Selection selection) throws CoordinatorException, RecoveryException, ExecutionException {
    // Arrange
    TransactionResult result =
        prepareResult(
            System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS * 2,
            TransactionState.PREPARED,
            false);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(storage.get(any())).thenReturn(Optional.of(result));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    Optional<TransactionResult> actual = handler.recover(selection, result);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(handler).rollbackRecord(selection, result);
    assertThat(actual).isNotPresent();
  }

  private void assertAfterImageResult(Optional<TransactionResult> actual) {
    assertThat(actual).isPresent();
    assertThat(actual.get().getValue(ANY_NAME_1).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_1).get())
        .isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get().getValue(ANY_NAME_2).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_2).get())
        .isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get().getValue(ANY_NAME_3).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_3).get())
        .isEqualTo(new TextValue(ANY_NAME_3, ANY_TEXT_3));
    assertThat(actual.get().getId()).isEqualTo(ANY_ID_1);
    assertThat(actual.get().getVersion()).isEqualTo(2);
    assertThat(actual.get().getPreparedAt()).isEqualTo(ANY_TIME_1);
    assertThat(actual.get().getCommittedAt()).isEqualTo(0);
  }

  private void assertBeforeImageResult(Optional<TransactionResult> actual) {
    assertThat(actual).isPresent();
    assertThat(actual.get().getValue(ANY_NAME_1).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_1).get())
        .isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get().getValue(ANY_NAME_2).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_2).get())
        .isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get().getValue(ANY_NAME_3).isPresent()).isTrue();
    assertThat(actual.get().getValue(ANY_NAME_3).get())
        .isEqualTo(new TextValue(ANY_NAME_3, ANY_TEXT_4));
    assertThat(actual.get().getId()).isEqualTo(ANY_ID_2);
    assertThat(actual.get().getVersion()).isEqualTo(1);
    assertThat(actual.get().getPreparedAt()).isEqualTo(ANY_TIME_2);
    assertThat(actual.get().getCommittedAt()).isEqualTo(ANY_TIME_2);
  }
}
