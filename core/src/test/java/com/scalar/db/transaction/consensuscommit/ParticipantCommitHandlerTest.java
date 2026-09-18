package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Direct unit tests for {@link ParticipantCommitHandler}. */
class ParticipantCommitHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_ID = "id";
  private static final String ANY_ID_2 = "id2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final long ANY_PREPARED_AT = 1000;
  private static final long ANY_COMMITTED_AT = 2000;
  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  @Mock private DistributedStorage storage;
  @Mock private RecoveryExecutor recoveryExecutor;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private StorageInfoProvider storageInfoProvider;
  @Mock private ConsensusCommitConfig config;

  private ParallelExecutor parallelExecutor;
  private MutationsGrouper mutationsGrouper;
  private ParticipantCommitHandler handler;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    parallelExecutor = new ParallelExecutor(config);
    mutationsGrouper = spy(new MutationsGrouper(storageInfoProvider));
    handler = newHandler(/* onePhaseCommitEnabled= */ false);

    when(storageInfoProvider.getStorageInfo(ANY_NAMESPACE_NAME))
        .thenReturn(
            new StorageInfoImpl(
                "storage1", StorageInfo.MutationAtomicityUnit.PARTITION, Integer.MAX_VALUE, false));
  }

  // Builds a ParticipantCommitHandler with the given one-phase-commit configuration. Tests that
  // exercise tryOnePhaseCommitRecords use the enabled variant; the rest use the default.
  private ParticipantCommitHandler newHandler(boolean onePhaseCommitEnabled) {
    return new ParticipantCommitHandler(
        storage,
        recoveryExecutor,
        tableMetadataManager,
        parallelExecutor,
        mutationsGrouper,
        onePhaseCommitEnabled);
  }

  @AfterEach
  void tearDown() {
    parallelExecutor.close();
  }

  private Put preparePut1() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Put preparePut2() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_4))
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Get prepareGet() {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_3))
        .build();
  }

  private Snapshot prepareSnapshot() {
    return new Snapshot(ANY_ID, tableMetadataManager, new ParallelExecutor(config));
  }

  private Snapshot prepareSnapshotWithDifferentPartitionPut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    Put put1 = preparePut1();
    Put put2 = preparePut2();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put2), put2);
    snapshot.putIntoGetSet(prepareGet(), Optional.empty());
    return snapshot;
  }

  private Snapshot prepareSnapshotWithoutWrites() {
    Snapshot snapshot = prepareSnapshot();
    snapshot.putIntoGetSet(prepareGet(), Optional.empty());
    return snapshot;
  }

  private TransactionContext createTransactionContext(Snapshot snapshot, Isolation isolation) {
    return new TransactionContext(ANY_ID, snapshot, isolation, false, false);
  }

  // ---------- prepareRecords ----------

  @Test
  void prepareRecords_WhenSuccessful_ShouldMutateStorage()
      throws ExecutionException, PreparationException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.prepareRecords(context, ANY_PREPARED_AT);

    // Assert
    verify(storage, times(2)).mutate(anyList());
  }

  @SuppressWarnings("unchecked")
  @Test
  void prepareRecords_ShouldStampPassedPreparedAtOnEveryPreparedRow()
      throws ExecutionException, PreparationException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.prepareRecords(context, ANY_PREPARED_AT);

    // Assert: every prepared row carries the single preparedAt passed in.
    ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(storage, times(2)).mutate(captor.capture());
    List<Mutation> preparedRows =
        captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(preparedRows).hasSize(2);
    for (Mutation mutation : preparedRows) {
      Put put = (Put) mutation;
      assertThat(put.getColumns().get(Attribute.PREPARED_AT).getBigIntValue())
          .isEqualTo(ANY_PREPARED_AT);
    }
  }

  @Test
  void prepareRecords_WhenNoMutationExceptionThrown_ShouldThrowPreparationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    throwNoMutationExceptionOnMutate();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  void
      prepareRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowPreparationConflictException()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  void prepareRecords_WhenExecutionExceptionThrown_ShouldThrowPreparationException()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- validateRecords ----------

  @Test
  void validateRecords_ValidationNotRequired_ShouldNotCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    // With SNAPSHOT isolation, validation is not required
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot, never()).toSerializable(storage);
  }

  @Test
  void validateRecords_ValidationRequired_ShouldCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(snapshot).toSerializable(storage);
    // With SERIALIZABLE isolation, validation is required when there are reads
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot).toSerializable(storage);
  }

  @Test
  void validateRecords_WhenExecutionExceptionThrown_ShouldThrowValidationException()
      throws ExecutionException, CrudException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act Assert
    assertThatThrownBy(() -> handler.validateRecords(context))
        .isInstanceOf(ValidationException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- commitRecords ----------

  @Test
  void commitRecords_WhenSuccessful_ShouldMutateStorage() throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.commitRecords(context, ANY_COMMITTED_AT);

    // Assert
    verify(storage, times(2)).mutate(anyList());
  }

  @SuppressWarnings("unchecked")
  @Test
  void commitRecords_ShouldStampPassedCommittedAtOnEveryCommittedRow()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.commitRecords(context, ANY_COMMITTED_AT);

    // Assert: every committed row carries the single committedAt passed in.
    ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(storage, times(2)).mutate(captor.capture());
    List<Mutation> committedRows =
        captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(committedRows).hasSize(2);
    for (Mutation mutation : committedRows) {
      Put put = (Put) mutation;
      assertThat(put.getColumns().get(Attribute.COMMITTED_AT).getBigIntValue())
          .isEqualTo(ANY_COMMITTED_AT);
    }
  }

  @Test
  void commitRecords_WhenStorageThrows_ShouldNotPropagateException()
      throws ExecutionException, CrudException {
    // Lazy recovery picks up failed commits, so commitRecords ignores storage failures.
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act (must not throw)
    handler.commitRecords(context, ANY_COMMITTED_AT);
  }

  // ---------- rollbackRecords ----------

  @Test
  void rollbackRecords_WhenSuccessful_ShouldDriveSnapshotThroughComposer()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.rollbackRecords(context);

    // Assert
    verify(snapshot).to(any(RollbackMutationComposer.class));
  }

  @Test
  void rollbackRecords_WhenStorageThrows_ShouldNotPropagateException()
      throws ExecutionException, CrudException {
    // Lazy recovery picks up failed rollbacks, so rollbackRecords ignores storage failures.
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act (must not throw)
    handler.rollbackRecords(context);
  }

  // ---------- canOnePhaseCommit ----------

  @Test
  void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    // Arrange — default handler has onePhaseCommitEnabled = false.
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(handler.canOnePhaseCommit(context)).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  void canOnePhaseCommit_WhenSerializableIsolationWithReads_ShouldReturnFalse() throws Exception {
    // SERIALIZABLE + reads requires validation, which one-phase commit cannot satisfy.
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithoutWrites();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse() throws Exception {
    // A delete with no corresponding record in the read set means we cannot detect conflicts
    // through delete-if-exists semantics.
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshot();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.empty());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doReturn(false).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenEligible_ShouldReturnTrue() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doReturn(true).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isTrue();
  }

  @Test
  void canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException()
      throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> enabled.canOnePhaseCommit(context))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- onePhaseCommitRecords ----------

  @Test
  void onePhaseCommitRecords_WhenSuccessful_ShouldMutateStorage() throws Exception {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.onePhaseCommitRecords(context);

    // Assert
    verify(storage).mutate(anyList());
    verify(snapshot).to(any(OnePhaseCommitMutationComposer.class));
  }

  @Test
  void onePhaseCommitRecords_WhenNoMutationException_ShouldThrowCommitConflictException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    throwNoMutationExceptionOnMutate();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  void onePhaseCommitRecords_WhenRetriableExecutionException_ShouldThrowCommitConflictException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  void onePhaseCommitRecords_WhenExecutionException_ShouldThrowUnknownTransactionStatusException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- recovering records blocking writes ----------

  private Put prepareInsertModePut() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_1)
        .enableInsertMode()
        .build();
  }

  // Same partition as prepareInsertModePut(), so both are grouped into a single storage call
  private Put prepareInsertModePutInSamePartition() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_4))
        .intValue(ANY_NAME_3, ANY_INT_2)
        .enableInsertMode()
        .build();
  }

  private Put prepareInsertModePutInDifferentPartition() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_4))
        .intValue(ANY_NAME_3, ANY_INT_2)
        .enableInsertMode()
        .build();
  }

  private Snapshot prepareSnapshotWithInsertModePut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    Put put = prepareInsertModePut();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    return snapshot;
  }

  private Snapshot prepareSnapshotWithInsertModePuts(Put put1, Put put2) throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put2), put2);
    return snapshot;
  }

  // A Put that is neither in insert mode nor read beforehand. The prepare composes it with a
  // PutIfNotExists condition, exactly like an insert, so it is blocked by the same records.
  private Snapshot prepareSnapshotWithBlindPut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    Put put = preparePut1();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    return snapshot;
  }

  // Makes storage.mutate() fail the way the storage adapters do: the thrown NoMutationException
  // carries the mutations of the failed call, which is what the recovery path filters on.
  private void throwNoMutationExceptionOnMutate() throws ExecutionException {
    doAnswer(
            invocation -> {
              List<Mutation> mutations = invocation.getArgument(0);
              throw new NoMutationException("No mutation was applied", mutations);
            })
        .when(storage)
        .mutate(anyList());
  }

  // Makes storage.mutate() fail with a NoMutationException for the partition of the given Put, and
  // with the given exception for the other partitions.
  private void throwNoMutationExceptionOnMutate(Put blockedPut, ExecutionException otherException)
      throws ExecutionException {
    doAnswer(
            invocation -> {
              List<Mutation> mutations = invocation.getArgument(0);
              if (mutations.get(0).getPartitionKey().equals(blockedPut.getPartitionKey())) {
                throw new NoMutationException("No mutation was applied", mutations);
              }
              throw otherException;
            })
        .when(storage)
        .mutate(anyList());
  }

  // Builds a ParticipantCommitHandler whose preparation behaves like parallel preparation in which
  // the task failing with an exception other than NoMutationException fails first: all the tasks
  // run to the end, and that exception is thrown with the NoMutationException suppressed in it.
  private ParticipantCommitHandler newHandlerFailingFirstWithOtherException()
      throws ExecutionException {
    ParallelExecutor parallelExecutor = mock(ParallelExecutor.class);
    doAnswer(
            invocation -> {
              List<ParallelExecutor.ParallelExecutorTask> tasks = invocation.getArgument(0);
              ExecutionException otherException = null;
              NoMutationException noMutationException = null;
              for (ParallelExecutor.ParallelExecutorTask task : tasks) {
                try {
                  task.run();
                } catch (NoMutationException e) {
                  noMutationException = e;
                } catch (ExecutionException e) {
                  otherException = e;
                }
              }
              assertThat(otherException).isNotNull();
              assertThat(noMutationException).isNotNull();
              otherException.addSuppressed(noMutationException);
              throw otherException;
            })
        .when(parallelExecutor)
        .prepareRecords(anyList(), anyString());
    return new ParticipantCommitHandler(
        storage,
        recoveryExecutor,
        tableMetadataManager,
        parallelExecutor,
        mutationsGrouper,
        /* onePhaseCommitEnabled= */ false);
  }

  private Result prepareExistingRecord(String transactionId, TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, transactionId))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, state.get()))
            .put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, 1))
            .build();
    return new ResultImpl(columns, TABLE_METADATA);
  }

  private RecoveryExecutor.Result prepareRecoveryResult(Put put) {
    return new RecoveryExecutor.Result(
        new Snapshot.Key(put), Optional.empty(), CompletableFuture.completedFuture(null), true);
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndBlockingRecordUncommittedByAnotherTransaction_ShouldExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(prepareInsertModePut());
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);

    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndBlockingRecordDeletedByAnotherTransaction_ShouldExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.DELETED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(prepareInsertModePut());
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }

  @Test
  void prepareRecords_WhenNoMutationExceptionThrownAndWriteIsBlindPut_ShouldExecuteRecovery()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithBlindPut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(preparePut1());
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndMultipleWritesBlocked_ShouldExecuteRecoveryForEach()
          throws Exception {
    // Arrange
    Snapshot snapshot =
        prepareSnapshotWithInsertModePuts(
            prepareInsertModePut(), prepareInsertModePutInSamePartition());
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    when(recoveryExecutor.execute(any(), any(), any(), any(), any()))
        .thenReturn(prepareRecoveryResult(prepareInsertModePut()));
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    // Both writes are in the same partition, so they are in the same failed storage call
    verify(storage, times(2)).get(any(Get.class));
    verify(recoveryExecutor, times(2)).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).hasSize(2);
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndOtherWriteNotInFailedCall_ShouldNotReadOtherRecord()
          throws Exception {
    // Arrange
    Snapshot snapshot =
        prepareSnapshotWithInsertModePuts(
            prepareInsertModePut(), prepareInsertModePutInDifferentPartition());
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    when(recoveryExecutor.execute(any(), any(), any(), any(), any()))
        .thenReturn(prepareRecoveryResult(prepareInsertModePut()));
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    // The writes are in different partitions, so they are in different storage calls. Only the
    // mutations of the failed call are examined
    verify(storage, times(1)).get(any(Get.class));
    verify(recoveryExecutor, times(1)).execute(any(), any(), any(), any(), any());
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownForOneOfWritesInParallel_ShouldNotReadRecordOfAppliedWrite()
          throws Exception {
    // Arrange

    // With parallel preparation, all the tasks run to the end, so the write in the other partition
    // is applied even though the storage call for the blocked write fails
    ConsensusCommitConfig parallelPreparationConfig = mock(ConsensusCommitConfig.class);
    when(parallelPreparationConfig.isParallelPreparationEnabled()).thenReturn(true);
    ParallelExecutor parallelExecutor =
        new ParallelExecutor(parallelPreparationConfig, Executors.newFixedThreadPool(2));
    ParticipantCommitHandler participantCommitHandler =
        new ParticipantCommitHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            parallelExecutor,
            mutationsGrouper,
            /* onePhaseCommitEnabled= */ false);
    Put blockedPut = prepareInsertModePut();
    Put appliedPut = prepareInsertModePutInDifferentPartition();
    Snapshot snapshot = prepareSnapshotWithInsertModePuts(blockedPut, appliedPut);
    doAnswer(
            invocation -> {
              List<Mutation> mutations = invocation.getArgument(0);
              if (mutations.get(0).getPartitionKey().equals(blockedPut.getPartitionKey())) {
                throw new NoMutationException("No mutation was applied", mutations);
              }
              return null;
            })
        .when(storage)
        .mutate(anyList());
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    when(recoveryExecutor.execute(any(), any(), any(), any(), any()))
        .thenReturn(prepareRecoveryResult(blockedPut));
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    try {
      assertThatThrownBy(() -> participantCommitHandler.prepareRecords(context, ANY_PREPARED_AT))
          .isInstanceOf(PreparationConflictException.class);
    } finally {
      parallelExecutor.close();
    }

    // Both storage calls are executed, but only the record of the write in the failed call is read
    verify(storage, times(2)).mutate(anyList());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(new Snapshot.Key(captor.getValue())).isEqualTo(new Snapshot.Key(blockedPut));
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionSuppressedInRetriableExecutionException_ShouldExecuteRecovery()
          throws Exception {
    // Arrange
    ParticipantCommitHandler participantCommitHandler = newHandlerFailingFirstWithOtherException();
    Put blockedPut = prepareInsertModePut();
    Snapshot snapshot =
        prepareSnapshotWithInsertModePuts(blockedPut, prepareInsertModePutInDifferentPartition());
    throwNoMutationExceptionOnMutate(
        blockedPut, new RetriableExecutionException("A conflict occurred"));
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(blockedPut);
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> participantCommitHandler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);

    // The record blocking the write whose NoMutationException is suppressed should be recovered
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(new Snapshot.Key(captor.getValue())).isEqualTo(new Snapshot.Key(blockedPut));
    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }

  @Test
  void prepareRecords_WhenNoMutationExceptionSuppressedInExecutionException_ShouldExecuteRecovery()
      throws Exception {
    // Arrange
    ParticipantCommitHandler participantCommitHandler = newHandlerFailingFirstWithOtherException();
    Put blockedPut = prepareInsertModePut();
    Snapshot snapshot =
        prepareSnapshotWithInsertModePuts(blockedPut, prepareInsertModePutInDifferentPartition());
    throwNoMutationExceptionOnMutate(blockedPut, new ExecutionException("An error occurred"));
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(blockedPut);
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> participantCommitHandler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationException.class)
        .isNotInstanceOf(PreparationConflictException.class)
        .hasCauseExactlyInstanceOf(ExecutionException.class);

    // The record blocking the write whose NoMutationException is suppressed should be recovered
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(new Snapshot.Key(captor.getValue())).isEqualTo(new Snapshot.Key(blockedPut));
    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndBlockingRecordCommitted_ShouldNotExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.COMMITTED)));
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(recoveryExecutor, never()).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).isEmpty();
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndBlockingRecordPreparedByItself_ShouldNotExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID, TransactionState.PREPARED)));
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(recoveryExecutor, never()).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).isEmpty();
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndBlockingRecordNotExist_ShouldNotExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(recoveryExecutor, never()).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).isEmpty();
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndFailedMutationNotConditionedOnAbsence_ShouldNotReadRecord()
          throws Exception {
    // Arrange

    // A write to a record that was read beforehand is composed with a PutIf condition on the
    // transaction ID instead of PutIfNotExists (see PrepareMutationComposerTest). Such a failure
    // means another transaction modified the record, which recovery cannot resolve, so the record
    // must not be read.
    Snapshot snapshot = prepareSnapshot();
    Put put = preparePut1();
    Snapshot.Key key = new Snapshot.Key(put);
    snapshot.putIntoReadSet(
        key,
        Optional.of(
            new TransactionResult(prepareExistingRecord(ANY_ID_2, TransactionState.COMMITTED))));
    snapshot.putIntoWriteSet(key, put);
    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    throwNoMutationExceptionOnMutate();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    verify(storage, never()).get(any(Get.class));
    verify(recoveryExecutor, never()).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).isEmpty();
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownAndReadingRecordFails_ShouldThrowPreparationConflictException()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class))).thenThrow(ExecutionException.class);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);

    verify(recoveryExecutor, never()).execute(any(), any(), any(), any(), any());
    assertThat(context.recoveryResults).isEmpty();
  }

  // Mimics a storage that converts the mutations before executing them, which the JDBC storage does
  // for a virtual table, rewriting a mutation into mutations for the tables the view is made of.
  // The exception such a storage throws holds the converted mutations.
  private List<Mutation> convertMutations(List<Mutation> mutations) {
    return mutations.stream()
        .map(
            mutation -> {
              Put put = (Put) mutation;
              PutBuilder.Buildable builder =
                  Put.newBuilder()
                      .namespace(ANY_NAMESPACE_NAME)
                      .table(ANY_TABLE_NAME + "_data")
                      .partitionKey(put.getPartitionKey());
              put.getClusteringKey().ifPresent(builder::clusteringKey);
              put.getColumns().values().forEach(builder::value);
              put.getCondition().ifPresent(builder::condition);
              return (Mutation) builder.build();
            })
        .collect(Collectors.toList());
  }

  @Test
  void
      prepareRecords_WhenNoMutationExceptionThrownWithConvertedMutations_ShouldReadRecordOfComposedMutation()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    doAnswer(
            invocation -> {
              List<Mutation> mutations = invocation.getArgument(0);
              throw new NoMutationException("No mutation was applied", convertMutations(mutations));
            })
        .when(storage)
        .mutate(anyList());
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(prepareInsertModePut());
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class);

    // The record of the mutation this transaction composed must be read, not the one of the
    // converted mutation the exception holds, since only the former has the transaction metadata
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(captor.getValue().forTable()).hasValue(ANY_TABLE_NAME);
    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
  }

  @Test
  void
      onePhaseCommitRecords_WhenNoMutationExceptionAndBlockingRecordUncommittedByAnotherTransaction_ShouldExecuteRecovery()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithInsertModePut();
    throwNoMutationExceptionOnMutate();
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(prepareExistingRecord(ANY_ID_2, TransactionState.PREPARED)));
    RecoveryExecutor.Result recoveryResult = prepareRecoveryResult(prepareInsertModePut());
    when(recoveryExecutor.execute(any(), any(), any(), any(), any())).thenReturn(recoveryResult);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);

    verify(recoveryExecutor)
        .execute(
            any(Snapshot.Key.class),
            any(Get.class),
            any(TransactionResult.class),
            eq(ANY_ID),
            eq(RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER));
    assertThat(context.recoveryResults).containsExactly(recoveryResult);
  }
}
