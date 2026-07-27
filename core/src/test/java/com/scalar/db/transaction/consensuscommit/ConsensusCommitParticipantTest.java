package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ConsensusCommitParticipantTest {
  private static final String ANY_NAMESPACE = "ns";
  private static final String ANY_TABLE = "tbl";
  private static final String ANY_PARTICIPANT_ID = "participant-A";
  private static final String ANY_TX_ID = "tx-1";

  @Mock private ConsensusCommitConfig config;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private RecoveryExecutor recoveryExecutor;
  @Mock private CrudHandler crud;
  @Mock private ParticipantCommitHandler commit;
  @Mock private ConsensusCommitOperationChecker operationChecker;
  @Mock private TransactionTableMetadata txTableMetadata;

  private ConsensusCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.getParticipantId()).thenReturn(Optional.of(ANY_PARTICIPANT_ID));
    when(config.getIsolation()).thenReturn(Isolation.SNAPSHOT);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("v", DataType.INT)
            .addPartitionKey("pk")
            .build();
    when(txTableMetadata.getTableMetadata()).thenReturn(tableMetadata);
    when(tableMetadataManager.getTransactionTableMetadata(any())).thenReturn(txTableMetadata);

    participant =
        new ConsensusCommitParticipant(
            config,
            tableMetadataManager,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit,
            operationChecker);
  }

  @Test
  void hasTransactionContext_ShouldReflectContextLifecycle() throws Exception {
    assertThat(participant.hasTransactionContext(ANY_TX_ID)).isFalse();

    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    assertThat(participant.hasTransactionContext(ANY_TX_ID)).isTrue();

    participant.releaseContext(ANY_TX_ID);
    assertThat(participant.hasTransactionContext(ANY_TX_ID)).isFalse();
  }

  @Test
  void constructor_WithoutParticipantId_ShouldThrowIllegalArgumentException() {
    when(config.getParticipantId()).thenReturn(Optional.empty());
    assertThatThrownBy(
            () ->
                new ConsensusCommitParticipant(
                    config,
                    tableMetadataManager,
                    parallelExecutor,
                    recoveryExecutor,
                    crud,
                    commit,
                    operationChecker))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void join_NewTransaction_ShouldCreateContext() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    // No exception means the context was created. A subsequent CRUD operation should find it.
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();
    doReturn(Optional.empty()).when(crud).get(eq(get), any(TransactionContext.class));
    Optional<Result> result = participant.get(ANY_TX_ID, get);
    assertThat(result).isEmpty();
    verify(crud).get(eq(get), any(TransactionContext.class));
  }

  @Test
  void join_WithTransactionIsolationAttribute_ShouldUseThatIsolationInsteadOfConfigDefault()
      throws Exception {
    // The config default is SNAPSHOT (see setUp); the attribute requests SERIALIZABLE for this tx.
    Map<String, String> attributes = new HashMap<>();
    ConsensusCommitOperationAttributes.setTransactionIsolation(attributes, Isolation.SERIALIZABLE);

    participant.join(ANY_TX_ID, false, attributes);

    assertThat(getContext(ANY_TX_ID).isolation).isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  void join_WithoutTransactionIsolationAttribute_ShouldUseConfigDefault() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());

    assertThat(getContext(ANY_TX_ID).isolation).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  void join_SameTransactionIdTwice_ShouldThrowTransactionException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    assertThatThrownBy(() -> participant.join(ANY_TX_ID, false, Collections.emptyMap()))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  void get_OnUnknownTransactionId_ShouldThrowTransactionNotFoundException() {
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();
    assertThatThrownBy(() -> participant.get("unknown-tx", get))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void put_ShouldDelegateToCrudHandler() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();
    doNothing().when(crud).put(any(Put.class), any(TransactionContext.class));

    participant.put(ANY_TX_ID, put);

    verify(crud).put(eq(put), any(TransactionContext.class));
  }

  @Test
  void put_OnReadOnlyTransaction_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();

    assertThatThrownBy(() -> participant.put(ANY_TX_ID, put))
        .isInstanceOf(IllegalStateException.class);
    verify(crud, never()).put(any(Put.class), any(TransactionContext.class));
  }

  @Test
  void update_WithoutConditionAndCrudThrowsUnsatisfiedCondition_ShouldDoNothing() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();
    // The record does not satisfy the implicit PutIfExists. With no user condition, this is a
    // no-op (the record does not exist), so the exception is swallowed.
    doThrow(UnsatisfiedConditionException.class)
        .when(crud)
        .put(any(Put.class), any(TransactionContext.class));

    assertThatCode(() -> participant.update(ANY_TX_ID, update)).doesNotThrowAnyException();
  }

  @Test
  void update_WithUpdateIfConditionAndCrudThrowsUnsatisfiedCondition_ShouldThrowConverted()
      throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column("v").isEqualToInt(1)).build())
            .build();
    UnsatisfiedConditionException crudException = mock(UnsatisfiedConditionException.class);
    when(crudException.getMessage()).thenReturn("PutIf");
    doThrow(crudException).when(crud).put(any(Put.class), any(TransactionContext.class));

    // The user supplied an UpdateIf condition, so the failure is rethrown with the message
    // converted back from the internal PutIf wording to UpdateIf.
    assertThatThrownBy(() -> participant.update(ANY_TX_ID, update))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("UpdateIf")
        .hasMessageNotContaining("PutIf");
  }

  @Test
  void delete_OnReadOnlyTransaction_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();

    assertThatThrownBy(() -> participant.delete(ANY_TX_ID, delete))
        .isInstanceOf(IllegalStateException.class);
    verify(crud, never()).delete(any(Delete.class), any(TransactionContext.class));
  }

  @Test
  void mutate_OnReadOnlyTransaction_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();

    assertThatThrownBy(() -> participant.mutate(ANY_TX_ID, Collections.singletonList(insert)))
        .isInstanceOf(IllegalStateException.class);
    verify(crud, never()).put(any(Put.class), any(TransactionContext.class));
  }

  @Test
  void batch_WithMutationOnReadOnlyTransaction_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 2))
            .intValue("v", 100)
            .build();

    assertThatThrownBy(() -> participant.batch(ANY_TX_ID, Collections.singletonList(insert)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void get_OnReadOnlyTransaction_ShouldBeAllowed() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();
    when(crud.get(any(Get.class), any(TransactionContext.class))).thenReturn(Optional.empty());

    assertThat(participant.get(ANY_TX_ID, get)).isEmpty();
    verify(crud).get(eq(get), any(TransactionContext.class));
  }

  @Test
  void prepareRecords_OnUnknownTransactionId_ShouldThrowTransactionNotFoundException() {
    // An absent local context (e.g., expired) surfaces as not-found.
    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    "unknown-tx", 1234L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void prepareRecords_WhenScannerNotClosed_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());

    // Open a scanner and leave it unclosed: register a non-closed scanner into the transaction
    // context, mirroring what the real CrudHandler.getScanner does.
    ConsensusCommitScanner openScanner = mock(ConsensusCommitScanner.class);
    when(openScanner.isClosed()).thenReturn(false);
    when(crud.getScanner(any(Scan.class), any(TransactionContext.class)))
        .thenAnswer(
            invocation -> {
              TransactionContext context = invocation.getArgument(1);
              context.scanners.add(openScanner);
              return openScanner;
            });
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();
    participant.getScanner(ANY_TX_ID, scan);

    // prepareRecords must reject a transaction that still has an open scanner ...
    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(IllegalStateException.class);
    // ... and the record-level prepare is never driven.
    verify(commit, never()).prepareRecords(any(TransactionContext.class), anyLong());
  }

  private static Scan anyScan() {
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE)
        .partitionKey(Key.ofInt("pk", 1))
        .build();
  }

  // Registers a (non-closed) raw scanner with the transaction context, mirroring the real
  // CrudHandler.getScanner, and returns the scanner the participant hands back (a pc-synchronized
  // wrapper around it).
  private TransactionCrudOperable.Scanner openScanner(ConsensusCommitScanner rawScanner)
      throws Exception {
    when(rawScanner.isClosed()).thenReturn(false);
    when(crud.getScanner(any(Scan.class), any(TransactionContext.class)))
        .thenAnswer(
            invocation -> {
              TransactionContext context = invocation.getArgument(1);
              context.scanners.add(rawScanner);
              return rawScanner;
            });
    return participant.getScanner(ANY_TX_ID, anyScan());
  }

  @Test
  void getScanner_ReturnedScanner_ShouldDelegateToRawScanner() throws Exception {
    participant.join(ANY_TX_ID, true, Collections.emptyMap());
    ConsensusCommitScanner raw = mock(ConsensusCommitScanner.class);
    when(raw.one()).thenReturn(Optional.empty());
    TransactionCrudOperable.Scanner scanner = openScanner(raw);

    scanner.one();
    scanner.close();

    verify(raw).one();
    verify(raw).close();
  }

  @Test
  void scanner_AfterContextReleased_ShouldThrowCrudConflictException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    TransactionCrudOperable.Scanner scanner = openScanner(mock(ConsensusCommitScanner.class));

    // rollbackRecords releases (and removes) the context. A subsequent iteration of the previously
    // obtained scanner must not read through the released context; it fails with a retriable
    // conflict instead.
    participant.rollbackRecords(ANY_TX_ID);

    assertThatThrownBy(scanner::one).isInstanceOf(CrudConflictException.class);
    assertThatThrownBy(scanner::all).isInstanceOf(CrudConflictException.class);
    // Iteration must be guarded too: iterator() drives the wrapper's one() per element, so a
    // released context surfaces the retriable conflict (wrapped, since Iterator throws no checked
    // exception) rather than reading through the released, closed underlying scanner.
    assertThatThrownBy(() -> scanner.iterator().hasNext())
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(CrudConflictException.class);
  }

  @Test
  void scannerIteration_ShouldBeMutuallyExclusiveWithContextRelease() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());

    CountDownLatch insideOne = new CountDownLatch(1);
    CountDownLatch releaseOne = new CountDownLatch(1);
    ConsensusCommitScanner raw = mock(ConsensusCommitScanner.class);
    // The raw one() blocks while the iterating thread holds the pc monitor, so a concurrent
    // rollbackRecords (which also locks pc to close scanners and release the context) cannot run
    // until iteration finishes.
    when(raw.one())
        .thenAnswer(
            invocation -> {
              insideOne.countDown();
              releaseOne.await();
              return Optional.empty();
            });
    TransactionCrudOperable.Scanner scanner = openScanner(raw);

    Thread iterating =
        new Thread(
            () -> {
              try {
                scanner.one();
              } catch (Exception ignored) {
                // not relevant to this test
              }
            });
    iterating.start();
    assertThat(insideOne.await(2, TimeUnit.SECONDS)).isTrue();

    AtomicBoolean rolledBack = new AtomicBoolean(false);
    Thread rollingBack =
        new Thread(
            () -> {
              try {
                participant.rollbackRecords(ANY_TX_ID);
                rolledBack.set(true);
              } catch (Exception ignored) {
                // not relevant to this test
              }
            });
    rollingBack.start();

    // While the iterating thread is inside one() (holding pc), rollbackRecords must be blocked.
    Thread.sleep(300);
    assertThat(rolledBack.get()).isFalse();

    // Let iteration finish; rollbackRecords can then acquire pc and complete.
    releaseOne.countDown();
    iterating.join(2000);
    rollingBack.join(2000);
    assertThat(rolledBack.get()).isTrue();
  }

  @Test
  void prepareRecords_WriteLessAndNoValidation_ShouldReturnEmptyResultAndReleaseContext()
      throws Exception {
    // Read-only, SNAPSHOT isolation: no writes and no validation required.
    participant.join(ANY_TX_ID, true, Collections.emptyMap());

    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());

    TwoPhaseCommit.PreparationResult result =
        participant.prepareRecords(ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);
    assertThat(result.getWriteSet()).isEmpty();
    assertThat(result.isValidationRequired()).isFalse();
    // No writes -> commit not required; parity with !getWriteSet().isEmpty().
    assertThat(result.isCommitRequired()).isFalse();
    verify(commit).prepareRecords(any(TransactionContext.class), eq(1000L));

    // The Coordinator will skip both validateRecords and commitRecords, so prepareRecords is this
    // participant's last step: the context is released, and the entry removed from the map.
    assertThat(getContext(ANY_TX_ID)).isNull();
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void prepareRecords_WriteLessButValidationRequired_ShouldKeepContextForValidation()
      throws Exception {
    // SERIALIZABLE with a recorded read but no writes: validation is required, so prepareRecords is
    // not the last step and the context must survive.
    when(config.getIsolation()).thenReturn(Isolation.SERIALIZABLE);
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    recordReadForValidation(ANY_TX_ID);

    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());

    TwoPhaseCommit.PreparationResult result =
        participant.prepareRecords(ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);
    assertThat(result.getWriteSet()).isEmpty();
    assertThat(result.isValidationRequired()).isTrue();
    // No writes -> commit not required, even though validation is.
    assertThat(result.isCommitRequired()).isFalse();
    // Context kept, so validateRecords can run.
    assertThat(getContext(ANY_TX_ID)).isNotNull();
  }

  @Test
  void validateRecords_WriteLess_ShouldReleaseContext() throws Exception {
    // A write-less participant that required validation: validateRecords is its last step (the
    // Coordinator skips commitRecords), so it releases the context there.
    when(config.getIsolation()).thenReturn(Isolation.SERIALIZABLE);
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    recordReadForValidation(ANY_TX_ID);

    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());
    participant.prepareRecords(ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);

    participant.validateRecords(ANY_TX_ID);

    verify(commit).validateRecords(any(TransactionContext.class));
    // Released at validateRecords: a subsequent commitRecords finds no context.
    assertThat(getContext(ANY_TX_ID)).isNull();
    assertThatThrownBy(() -> participant.commitRecords(ANY_TX_ID, 2000L))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void validateRecords_WriteLess_WhenValidationFails_ShouldKeepContextForRollback()
      throws Exception {
    // A write-less SERIALIZABLE participant whose validation fails: on the throw path neither
    // toValidated() nor the write-less release() runs, so the context must survive for the
    // Coordinator's abort path to drive rollbackRecords on it.
    when(config.getIsolation()).thenReturn(Isolation.SERIALIZABLE);
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    recordReadForValidation(ANY_TX_ID);

    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());
    participant.prepareRecords(ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);

    doThrow(new ValidationConflictException("conflict", ANY_TX_ID))
        .when(commit)
        .validateRecords(any(TransactionContext.class));

    // validateRecords surfaces the conflict ...
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(ValidationConflictException.class);

    // ... and the context survives, so rollbackRecords can still undo the (write-less) prepare.
    assertThat(getContext(ANY_TX_ID)).isNotNull();
    participant.rollbackRecords(ANY_TX_ID);
    verify(commit).rollbackRecords(any(TransactionContext.class));
  }

  @Test
  void prepareRecords_WithWriteAndDelete_ShouldReturnEntriesStampedWithParticipantId()
      throws Exception {
    TwoPhaseCommit.PreparationResult result =
        prepareRecordsWithWriteAndDelete(TwoPhaseCommit.WriteSetDetailLevel.FULL);
    // Has writes -> commit required; parity with !getWriteSet().isEmpty().
    assertThat(result.isCommitRequired()).isTrue();
    List<TwoPhaseCommit.WriteSetEntry> entries = result.getWriteSet();
    assertThat(entries).hasSize(2);
    assertThat(participant.getId()).isEqualTo(ANY_PARTICIPANT_ID);
    assertThat(entries)
        .extracting(TwoPhaseCommit.WriteSetEntry::getType)
        .containsExactlyInAnyOrder(
            TwoPhaseCommit.WriteSetEntry.Type.WRITE, TwoPhaseCommit.WriteSetEntry.Type.DELETE);
    assertThat(entries)
        .allSatisfy(
            e -> {
              assertThat(e.getNamespaceName()).isEqualTo(ANY_NAMESPACE);
              assertThat(e.getTableName()).isEqualTo(ANY_TABLE);
            });
  }

  @Test
  void prepareRecords_Full_ShouldCarryUserColumnsAndExcludeTransactionMetaColumns()
      throws Exception {
    TwoPhaseCommit.PreparationResult result =
        prepareRecordsWithWriteAndDelete(TwoPhaseCommit.WriteSetDetailLevel.FULL);
    List<TwoPhaseCommit.WriteSetEntry> entries = result.getWriteSet();
    assertThat(entries).hasSize(2);
    for (TwoPhaseCommit.WriteSetEntry entry : entries) {
      if (entry.getType() == TwoPhaseCommit.WriteSetEntry.Type.WRITE) {
        // The injected Put carries the user column "v" plus the ConsensusCommit-internal "tx_id";
        // only the user column may appear in the write set.
        assertThat(entry.getColumns()).extracting(Column::getName).containsExactly("v");
      } else {
        // DELETE entries never carry columns.
        assertThat(entry.getColumns()).isEmpty();
      }
    }
  }

  @Test
  void prepareRecords_KeysOnly_ShouldReturnSameEntriesWithoutColumns() throws Exception {
    TwoPhaseCommit.PreparationResult result =
        prepareRecordsWithWriteAndDelete(TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);
    // KEYS_ONLY never changes which entries are returned — only their column payload: same entry
    // count and keys as FULL, but every getColumns() is empty (the must-not-carry-columns
    // contract).
    List<TwoPhaseCommit.WriteSetEntry> entries = result.getWriteSet();
    assertThat(entries).hasSize(2);
    assertThat(entries)
        .extracting(TwoPhaseCommit.WriteSetEntry::getType)
        .containsExactlyInAnyOrder(
            TwoPhaseCommit.WriteSetEntry.Type.WRITE, TwoPhaseCommit.WriteSetEntry.Type.DELETE);
    assertThat(entries)
        .allSatisfy(
            e -> {
              assertThat(e.getPartitionKey().getColumns()).isNotEmpty();
              assertThat(e.getColumns()).isEmpty();
            });
    // Skipping the column pass must also skip the table-metadata lookup (needed only to filter
    // transaction-meta columns), so a KEYS_ONLY prepare cannot fail on metadata retrieval.
    verify(tableMetadataManager, never()).getTransactionTableMetadata(any());
  }

  // Populates the snapshot with one write (user column "v" plus the transaction-meta column
  // "tx_id") and one delete, then drives prepareRecords at the given write-set detail.
  private TwoPhaseCommit.PreparationResult prepareRecordsWithWriteAndDelete(
      TwoPhaseCommit.WriteSetDetailLevel detailLevel) throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());

    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 2))
            .build();
    // Stub CrudHandler.put/delete to actually populate the snapshot via the context.
    doNothing().when(crud).put(any(Put.class), any(TransactionContext.class));
    doNothing().when(crud).delete(any(Delete.class), any(TransactionContext.class));
    participant.insert(ANY_TX_ID, insert);
    participant.delete(ANY_TX_ID, delete);

    // CrudHandler is mocked, so the snapshot stays empty unless we populate it directly. Reach
    // into the per-tx Snapshot and inject a write + delete, mirroring what the real CrudHandler
    // would do for the insert + delete above. The write additionally carries the "tx_id"
    // transaction-meta column so the FULL meta-column filtering is observable.
    TransactionContext context = getContext(ANY_TX_ID);
    Put put = Put.newBuilder(buildPutFromInsert(insert)).textValue("tx_id", "meta").build();
    context.snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    context.snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);

    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());

    return participant.prepareRecords(ANY_TX_ID, 2000L, detailLevel);
  }

  @Test
  void prepareRecords_AfterPrepare_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    // A second prepare is out of phase (no idempotency under the strict state machine).
    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 2000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void get_AfterPrepare_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .build();
    // CRUD after the two-phase commit phase has started is out of phase.
    assertThatThrownBy(() -> participant.get(ANY_TX_ID, get))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void prepareRecords_PreparationConflict_ShouldPropagateAndKeepContext() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doThrow(new PreparationConflictException("boom", ANY_TX_ID))
        .when(commit)
        .prepareRecords(any(TransactionContext.class), anyLong());

    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 3000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(PreparationConflictException.class);

    // Context kept; caller may still call rollbackRecords.
    assertThat(getContext(ANY_TX_ID)).isNotNull();
  }

  @Test
  void prepareRecords_WhenImplicitPreReadConflicts_ShouldThrowPreparationConflictException()
      throws Exception {
    // A conflict during the implicit pre-read must surface as a retriable PreparationConflict
    // (not a plain PreparationException), and commit.prepareRecords must not be reached.
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    doThrow(new CrudConflictException("pre-read conflict", ANY_TX_ID))
        .when(crud)
        .readIfImplicitPreReadEnabled(any(TransactionContext.class));

    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(PreparationConflictException.class);
    verify(commit, never()).prepareRecords(any(TransactionContext.class), anyLong());
  }

  @Test
  void prepareRecords_WhenImplicitPreReadFails_ShouldThrowNonConflictPreparationException()
      throws Exception {
    // A non-conflict failure during the implicit pre-read must surface as a (non-retriable)
    // PreparationException, not a PreparationConflictException.
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    doThrow(new CrudException("pre-read failed", ANY_TX_ID))
        .when(crud)
        .readIfImplicitPreReadEnabled(any(TransactionContext.class));

    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(PreparationException.class)
        .isNotInstanceOf(PreparationConflictException.class);
    verify(commit, never()).prepareRecords(any(TransactionContext.class), anyLong());
  }

  @Test
  void commitRecords_WhenValidated_ShouldCommitAndRemoveContext() throws Exception {
    // The normal protocol path the Coordinator drives is prepare -> validate -> commit, so
    // commitRecords is invoked from the VALIDATED state (not directly from PREPARED).
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepareAndValidate(ANY_TX_ID);
    participant.commitRecords(ANY_TX_ID, 5000L);
    verify(commit).commitRecords(any(TransactionContext.class), eq(5000L));
    // The context is removed after commit.
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void rollbackRecords_WhenValidated_ShouldRollBackRecordsAndRemoveContext() throws Exception {
    // VALIDATED still counts as prepared (isPrepared()), so a rollback from this state must undo
    // the PREPARED records in storage, like the PREPARED case.
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepareAndValidate(ANY_TX_ID);
    participant.rollbackRecords(ANY_TX_ID);
    verify(commit).rollbackRecords(any(TransactionContext.class));
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void rollbackRecords_AfterPrepareThrewPartway_ShouldRollBackPreparedRecords() throws Exception {
    // A partial prepare (prepareRecords throws after writing some PREPARED records) must still
    // leave the transaction marked PREPARED so rollbackRecords undoes those records in storage.
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doThrow(new PreparationConflictException("boom", ANY_TX_ID))
        .when(commit)
        .prepareRecords(any(TransactionContext.class), anyLong());
    assertThatThrownBy(
            () ->
                participant.prepareRecords(
                    ANY_TX_ID, 3000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY))
        .isInstanceOf(PreparationConflictException.class);

    participant.rollbackRecords(ANY_TX_ID);

    // The storage rollback runs even though prepareRecords did not complete.
    verify(commit).rollbackRecords(any(TransactionContext.class));
  }

  @Test
  void validateRecords_ShouldDelegateToParticipantCommitHandler() throws Exception {
    // prepare() injects a write, so this is the write-bearing validate path: the context is marked
    // VALIDATED and kept alive for the Coordinator to drive commitRecords (it is NOT released here,
    // unlike the write-less path in validateRecords_WriteLess_ShouldReleaseContext).
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    doNothing().when(commit).validateRecords(any(TransactionContext.class));
    participant.validateRecords(ANY_TX_ID);
    verify(commit).validateRecords(any(TransactionContext.class));
    assertThat(getContext(ANY_TX_ID)).isNotNull();
  }

  @Test
  void validateRecords_BeforePrepare_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void validateRecords_UnknownTransactionId_ShouldThrowTransactionNotFoundException() {
    // An absent local context (e.g., expired) surfaces as not-found.
    assertThatThrownBy(() -> participant.validateRecords("unknown-tx"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void commitRecords_ShouldRemoveContext() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    participant.commitRecords(ANY_TX_ID, 5000L);
    verify(commit).commitRecords(any(TransactionContext.class), eq(5000L));
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void commitRecords_BeforePrepare_ShouldThrowIllegalStateException() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    assertThatThrownBy(() -> participant.commitRecords(ANY_TX_ID, 5000L))
        .isInstanceOf(IllegalStateException.class);
    verify(commit, never()).commitRecords(any(TransactionContext.class), anyLong());
  }

  @Test
  void commitRecords_UnknownTransactionId_ShouldThrowTransactionNotFoundException() {
    // An absent local context (e.g., expired between prepare and commit) surfaces as not-found; the
    // Coordinator drives this step best-effort and lazy recovery reconciles.
    assertThatThrownBy(() -> participant.commitRecords("unknown-tx", 5000L))
        .isInstanceOf(TransactionNotFoundException.class);
    verify(commit, never()).commitRecords(any(TransactionContext.class), anyLong());
  }

  @Test
  void rollbackRecords_WhenPrepared_ShouldRollBackRecordsAndRemoveContext() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    participant.rollbackRecords(ANY_TX_ID);
    // Prepared, so the PREPARED records are rolled back in storage; the context is removed.
    verify(commit).rollbackRecords(any(TransactionContext.class));
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void rollbackRecords_WhenNotPrepared_ShouldReleaseContextWithoutStorageRollback()
      throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    participant.rollbackRecords(ANY_TX_ID);
    // Never prepared (still in the CRUD phase): no PREPARED records in storage, so no storage
    // rollback — only the in-memory context is released.
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void rollbackRecords_UnknownTransactionId_ShouldBeNoOp() {
    // Unlike the other record-level steps, rollbackRecords is lenient: an absent context (never
    // joined, already released when later steps were skipped, or a prior rollback) leaves nothing
    // to undo, so it is a no-op rather than an error.
    participant.rollbackRecords("unknown-tx");
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
  }

  @Test
  void releaseContext_WhenPrepared_ShouldReleaseContextWithoutStorageRollback() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);

    participant.releaseContext(ANY_TX_ID);

    // Reap-only terminal: even though the transaction is PREPARED, the records are NOT rolled back
    // in storage (lazy recovery reconciles them); only the in-memory context is released.
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    assertThat(getContext(ANY_TX_ID)).isNull();
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void releaseContext_WhenValidated_ShouldReleaseContextWithoutStorageRollback() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());
    prepare(ANY_TX_ID);
    doNothing().when(commit).validateRecords(any(TransactionContext.class));
    participant.validateRecords(ANY_TX_ID);

    participant.releaseContext(ANY_TX_ID);

    // Reap-only terminal: even a VALIDATED (write-bearing, commitRecords not yet driven) context is
    // NOT rolled back in storage — lazy recovery reconciles its PREPARED records; only the
    // in-memory context is released. VALIDATED is the primary reaper target state.
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    assertThat(getContext(ANY_TX_ID)).isNull();
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void releaseContext_WhenActive_ShouldReleaseContextWithoutStorageRollback() throws Exception {
    participant.join(ANY_TX_ID, false, Collections.emptyMap());

    participant.releaseContext(ANY_TX_ID);

    // Reap-only terminal on an ACTIVE (never prepared) context: only an in-memory snapshot exists,
    // so there is nothing to roll back in storage — the context is simply released.
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    assertThat(getContext(ANY_TX_ID)).isNull();
    assertThatThrownBy(() -> participant.validateRecords(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void releaseContext_UnknownTransactionId_ShouldBeNoOp() {
    // An absent context (never joined, or already released) leaves nothing to release: a no-op.
    participant.releaseContext("unknown-tx");
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
  }

  // Drives the transaction to the PREPARED state with the handlers stubbed to no-op. Injects a
  // write into the snapshot so the participant has writes: a write-bearing participant stays alive
  // through prepare and validate (a write-less one self-releases early under the
  // skip-optimization), which is the state the lifecycle tests below exercise.
  private void prepare(String txId) throws Exception {
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 100)
            .build();
    getContext(txId).snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    doNothing().when(crud).readIfImplicitPreReadEnabled(any(TransactionContext.class));
    doNothing().when(crud).waitForRecoveryCompletionIfNecessary(any(TransactionContext.class));
    doNothing().when(commit).prepareRecords(any(TransactionContext.class), anyLong());
    participant.prepareRecords(txId, 1000L, TwoPhaseCommit.WriteSetDetailLevel.KEYS_ONLY);
  }

  // Drives the transaction to the VALIDATED state — the normal path the Coordinator drives before
  // commitRecords/rollbackRecords (prepare -> validate -> commit/rollback).
  private void prepareAndValidate(String txId) throws Exception {
    prepare(txId);
    doNothing().when(commit).validateRecords(any(TransactionContext.class));
    participant.validateRecords(txId);
  }

  // Records a read (a scan) in the snapshot so that, under SERIALIZABLE isolation, validation is
  // required even without writes (Snapshot#isScanSetEmpty becomes false).
  private void recordReadForValidation(String txId) throws Exception {
    Scan scan = Scan.newBuilder().namespace(ANY_NAMESPACE).table(ANY_TABLE).all().build();
    getContext(txId).snapshot.putIntoScanSet(scan, new LinkedHashMap<>());
  }

  private TransactionContext getContext(String txId) throws Exception {
    // Reach into the participant via reflection — preferred over exposing a getter only used in
    // tests. The contexts map is the participant's intentionally-internal state.
    java.lang.reflect.Field field = ConsensusCommitParticipant.class.getDeclaredField("contexts");
    field.setAccessible(true);
    java.util.Map<?, ?> map = (java.util.Map<?, ?>) field.get(participant);
    Object pc = map.get(txId);
    if (pc == null) {
      return null;
    }
    java.lang.reflect.Method method = pc.getClass().getDeclaredMethod("context");
    method.setAccessible(true);
    return (TransactionContext) method.invoke(pc);
  }

  private Put buildPutFromInsert(Insert insert) {
    return ConsensusCommitUtils.createPutForInsert(insert);
  }
}
