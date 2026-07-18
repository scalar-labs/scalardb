package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ActiveTransactionManagedTwoPhaseCommitParticipantTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX = "tx-1";

  // A short expiration so reaping fires quickly; the registry sweeps roughly every second.
  private static final long EXPIRATION_MILLIS = 100;

  // Long enough to cover at least one post-expiration sweep, used to assert reaping does NOT
  // happen.
  private static final long PAST_SWEEP_MILLIS = 1500;

  @Mock private TwoPhaseCommitParticipant delegate;
  private ActiveTransactionManagedTwoPhaseCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    participant =
        new ActiveTransactionManagedTwoPhaseCommitParticipant(
            delegate, EXPIRATION_MILLIS, /* maxActiveTransactions= */ -1);
  }

  private static Get get() {
    return Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Insert insert() {
    return Insert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", 1))
        .intValue("v", 1)
        .build();
  }

  @Test
  void join_ShouldDelegateAndRegister_ThenReapReleasesContextOnDelegate() throws Exception {
    participant.join(TX, false, Collections.emptyMap());

    verify(delegate).join(TX, false, Collections.emptyMap());
    // After the idle expiration, the reaper releases the context on the wrapped participant.
    verify(delegate, timeout(PAST_SWEEP_MILLIS * 4)).releaseTransactionContext(TX);
  }

  @Test
  void join_WhenReleaseThrowsNotFound_ShouldTreatItAsAlreadyGoneAndKeepReaping() throws Exception {
    // A remote wrapped participant may report the reap's no-op outcome on its conventional
    // not-found channel (the contract's alternative carrier of "there was no context to
    // release"); the disposal path tolerates it, and the reaper keeps collecting other
    // transactions.
    doThrow(new TransactionNotFoundException("already gone", TX))
        .when(delegate)
        .releaseTransactionContext(TX);
    participant.join(TX, false, Collections.emptyMap());
    participant.join("tx-2", false, Collections.emptyMap());

    verify(delegate, timeout(PAST_SWEEP_MILLIS * 4)).releaseTransactionContext(TX);
    verify(delegate, timeout(PAST_SWEEP_MILLIS * 4)).releaseTransactionContext("tx-2");
  }

  @Test
  void disposalHandler_OnIdleExpiry_ShouldInvokeHandlerWithTransactionId_NotDefaultRelease()
      throws Exception {
    // The privileged-reap seam: when a disposal handler is supplied, the reaper invokes it with the
    // transaction ID instead of releasing the context on the wrapped participant itself, so an
    // embedder can run the release in its own execution context (e.g. a privileged mode).
    CountDownLatch disposed = new CountDownLatch(1);
    AtomicReference<String> disposedId = new AtomicReference<>();
    ActiveTransactionManagedTwoPhaseCommitParticipant p =
        new ActiveTransactionManagedTwoPhaseCommitParticipant(
            delegate,
            EXPIRATION_MILLIS,
            /* maxActiveTransactions= */ -1,
            transactionId -> {
              disposedId.set(transactionId);
              disposed.countDown();
            });

    p.join(TX, false, Collections.emptyMap());

    assertThat(disposed.await(PAST_SWEEP_MILLIS * 4, TimeUnit.MILLISECONDS)).isTrue();
    assertThat(disposedId.get()).isEqualTo(TX);
    // The handler replaced the default action: the reaper does not release the context on the
    // wrapped participant itself.
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void hasTransactionContext_ShouldForwardToDelegateUntouched() throws Exception {
    // Deliberately not overridden: the probe passes through to the wrapped participant so it can
    // never refresh this decorator's idle timer - the quiet contract holds by construction. This
    // pins that no registry-based override gets reintroduced.
    when(delegate.hasTransactionContext(TX)).thenReturn(true);

    assertThat(participant.hasTransactionContext(TX)).isTrue();

    verify(delegate).hasTransactionContext(TX);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void hasTransactionContext_ShouldNotTouchRegistry() throws Exception {
    // The quiet contract pinned from the registry side: the probe must not count as activity (no
    // touch, no get), or the probing coordinator would keep an abandoned transaction's idle timer
    // fresh forever. The forwarding test above stays green even for an override that also touches
    // the registry; this one does not.
    ActiveTransactionRegistry registry = mock(ActiveTransactionRegistry.class);
    ActiveTransactionManagedTwoPhaseCommitParticipant p =
        new ActiveTransactionManagedTwoPhaseCommitParticipant(delegate, registry);
    when(delegate.hasTransactionContext(TX)).thenReturn(true);

    assertThat(p.hasTransactionContext(TX)).isTrue();

    verifyNoInteractions(registry);
  }

  @Test
  void get_ShouldDelegate() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    Get get = get();

    participant.get(TX, get);

    verify(delegate).get(TX, get);
  }

  @Test
  void insert_ShouldDelegate() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    Insert insert = insert();

    participant.insert(TX, insert);

    verify(delegate).insert(TX, insert);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void getScanner_ReturnedScanner_ShouldRefreshIdleTimerOnEachRead() throws Exception {
    ActiveTransactionRegistry registry = mock(ActiveTransactionRegistry.class);
    ActiveTransactionManagedTwoPhaseCommitParticipant p =
        new ActiveTransactionManagedTwoPhaseCommitParticipant(delegate, registry);
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();
    TransactionCrudOperable.Scanner rawScanner = mock(TransactionCrudOperable.Scanner.class);
    when(rawScanner.one()).thenReturn(Optional.empty());
    when(rawScanner.all()).thenReturn(Collections.emptyList());
    when(delegate.getScanner(TX, scan)).thenReturn(rawScanner);

    TransactionCrudOperable.Scanner scanner = p.getScanner(TX, scan);
    scanner.one();
    scanner.all();

    // getScanner refreshes once; then each one()/all() on the returned scanner refreshes too, so a
    // slowly-consumed scan is not reaped mid-iteration.
    verify(registry, times(3)).touch(TX);
  }

  @Test
  void getScanner_ShouldDelegate() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();

    participant.getScanner(TX, scan);

    verify(delegate).getScanner(TX, scan);
  }

  // Stubs the wrapped participant's prepareRecords to report the given terminality, so the
  // decorator can decide where this transaction's terminal step lands.
  private void stubPrepare(boolean commitRequired, boolean validationRequired) throws Exception {
    TwoPhaseCommitParticipant.PreparationResult result =
        mock(TwoPhaseCommitParticipant.PreparationResult.class);
    when(result.isCommitRequired()).thenReturn(commitRequired);
    when(result.isValidationRequired()).thenReturn(validationRequired);
    when(delegate.prepareRecords(eq(TX), anyLong(), any())).thenReturn(result);
  }

  @Test
  void prepareRecords_ShouldDelegate() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(true, false); // commit required, so the entry stays until commitRecords

    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);

    verify(delegate)
        .prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
  }

  @Test
  void prepareRecords_WriteLessAndNoValidation_ShouldStopReapingAtPrepare() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(false, false);

    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);

    // prepareRecords is this participant's terminal step, so the entry is removed now: the reaper
    // never releases the context (and logs no spurious expiry warning).
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void prepareRecords_WriteLessButValidationRequired_ShouldStopReapingAtValidate()
      throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(false, true);

    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
    participant.validateRecords(TX); // terminal step for a write-less, validating participant

    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void commitRequired_ShouldStopReapingAtCommit() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(true, true);

    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
    participant.validateRecords(TX); // not terminal: commitRecords is still to come
    participant.commitRecords(TX, 1L); // terminal step

    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void rollbackRecords_AfterValidateTerminalFlagged_ShouldStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(false, true); // flags the entry terminal-at-validate
    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);

    // Abort before validate: rollbackRecords removes the entry (and with it the flag).
    participant.rollbackRecords(TX);

    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void validateRecords_WhenDelegateThrows_AndNoRollback_ShouldRemainReapable() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(false, true); // write-less but validating: validateRecords is the terminal step
    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
    doThrow(new ValidationException("boom", TX)).when(delegate).validateRecords(TX);

    try {
      participant.validateRecords(TX);
    } catch (ValidationException ignored) {
      // expected
    }

    // Removal is success-only: a failed validateRecords must NOT remove the entry (the abort path's
    // rollbackRecords owns cleanup). With no rollback driven here, the entry survives, so the
    // reaper still releases the context. This distinguishes "not removed on failure" from
    // "removed": had the failure wrongly removed the entry, releaseTransactionContext would never
    // fire and this would time out.
    verify(delegate, timeout(PAST_SWEEP_MILLIS * 4)).releaseTransactionContext(TX);
  }

  @Test
  void validateRecords_WhenDelegateThrows_ThenRollbackRecords_ShouldStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    stubPrepare(false, true); // write-less but validating: validateRecords is the terminal step
    participant.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
    doThrow(new ValidationException("boom", TX)).when(delegate).validateRecords(TX);

    try {
      participant.validateRecords(TX);
    } catch (ValidationException ignored) {
      // expected
    }

    // The documented abort path: validateRecords failed, so the transaction aborts and
    // rollbackRecords is driven, which removes the entry. The reaper must then not release it.
    participant.rollbackRecords(TX);
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void commitRecords_ShouldStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());

    participant.commitRecords(TX, 1L);

    verify(delegate).commitRecords(TX, 1L);
    // The transaction is no longer tracked, so the reaper must not release its context.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void rollbackRecords_ShouldStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());

    participant.rollbackRecords(TX);

    verify(delegate).rollbackRecords(TX);
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void commitRecords_WhenDelegateThrows_ShouldStillStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    doThrow(new CommitException("boom", TX)).when(delegate).commitRecords(TX, 1L);

    try {
      participant.commitRecords(TX, 1L);
    } catch (CommitException ignored) {
      // expected
    }

    // The finally block deregistered the transaction even though commitRecords threw, so the
    // reaper must not subsequently release its context (no double reap).
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void rollbackRecords_WhenDelegateThrows_ShouldStillStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    doThrow(new RollbackException("boom", TX)).when(delegate).rollbackRecords(TX);

    try {
      participant.rollbackRecords(TX);
    } catch (RollbackException ignored) {
      // expected
    }

    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(TX);
  }

  @Test
  void releaseTransactionContext_ShouldDelegateOnceAndStopReaping() throws Exception {
    participant.join(TX, false, Collections.emptyMap());

    participant.releaseTransactionContext(TX);

    // The reaper must not call releaseTransactionContext again after the client already released
    // it.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate).releaseTransactionContext(TX);
  }

  @Test
  void join_WhenDelegateThrows_ShouldNotRegister() throws Exception {
    doThrow(new TransactionException("boom", TX)).when(delegate).join(eq(TX), eq(false), any());

    try {
      participant.join(TX, false, Collections.emptyMap());
    } catch (TransactionException ignored) {
      // expected
    }

    // A failed join registered nothing, so the reaper must not release any context.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseTransactionContext(any());
  }

  // Deterministic wiring check (no Thread.sleep): every CRUD and record-level override must refresh
  // the idle timer via registry.touch(id). A mock registry verifies the call directly, so a dropped
  // touch() in any of these methods is caught immediately rather than silently shortening the
  // transaction's lifetime. This is the regression guard the *_ShouldDelegate tests do not provide:
  // those assert delegation only and stay green even if touch() is removed.
  @Test
  @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
  void everyCrudAndRecordStep_ShouldRefreshIdleTimer_ByCallingTouch() throws Exception {
    ActiveTransactionRegistry registry = mock(ActiveTransactionRegistry.class);
    // validateRecords reads the entry back; return empty so it takes the no-op (non-terminal) path.
    when(registry.get(TX)).thenReturn(Optional.empty());
    ActiveTransactionManagedTwoPhaseCommitParticipant p =
        new ActiveTransactionManagedTwoPhaseCommitParticipant(delegate, registry);

    Get get = get();
    Insert insert = insert();
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    Update update =
        Update.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    Delete delete =
        Delete.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    stubPrepare(true, false); // commit required: prepareRecords leaves the entry for commitRecords

    p.get(TX, get);
    p.scan(TX, scan);
    p.getScanner(TX, scan);
    p.put(TX, put); // deprecated, but still a touch()-bearing override that must be guarded
    p.insert(TX, insert);
    p.upsert(TX, upsert);
    p.update(TX, update);
    p.delete(TX, delete);
    p.mutate(TX, Collections.singletonList(insert));
    p.batch(TX, Collections.singletonList(insert));
    p.prepareRecords(TX, 100L, TwoPhaseCommitParticipant.WriteSetDetailLevel.KEYS_ONLY);
    p.validateRecords(TX);

    // One touch per operation above; if any override drops its touch(), this count fails.
    verify(registry, times(12)).touch(TX);
  }
}
