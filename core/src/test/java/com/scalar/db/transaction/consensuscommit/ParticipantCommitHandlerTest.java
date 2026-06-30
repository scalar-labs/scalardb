package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.StorageInfo;
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
import com.scalar.db.io.Key;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  @Mock private DistributedStorage storage;
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
        storage, tableMetadataManager, parallelExecutor, mutationsGrouper, onePhaseCommitEnabled);
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
    handler.prepareRecords(context);

    // Assert
    verify(storage, times(2)).mutate(anyList());
  }

  @Test
  void prepareRecords_WhenNoMutationExceptionThrown_ShouldThrowPreparationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context))
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
    assertThatThrownBy(() -> handler.prepareRecords(context))
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
    assertThatThrownBy(() -> handler.prepareRecords(context))
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
    handler.commitRecords(context);

    // Assert
    verify(storage, times(2)).mutate(anyList());
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
    handler.commitRecords(context);
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
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
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
}
