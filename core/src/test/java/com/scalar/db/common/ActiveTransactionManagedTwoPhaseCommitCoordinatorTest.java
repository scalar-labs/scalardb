package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitCoordinator.TrackedTransaction;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ActiveTransactionManagedTwoPhaseCommitCoordinatorTest {

  private static final String TX = "tx-1";

  // Long enough that no entry expires on its own during a test run: the deterministic tests force
  // an entry's expiration time to the past and invoke sweep() directly instead of waiting on the
  // clock (only the scheduling smoke test uses the wall clock).
  private static final long EXPIRATION_MILLIS = 60_000;

  @Mock private TwoPhaseCommitCoordinator delegate;
  @Mock private TwoPhaseCommitParticipant participant;

  private ActiveTransactionRegistry<TrackedTransaction> registry;
  private ActiveTransactionManagedTwoPhaseCommitCoordinator coordinator;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(delegate.begin(any(), eq(false), any(), any())).thenReturn(TX);
    when(participant.getId()).thenReturn("participant-1");
    // Cap-only in production too: the coordinator decorator disables idle expiration and owns
    // liveness itself, through the sweep.
    registry =
        new ActiveTransactionRegistry<>(
            /* expirationTimeMillis= */ -1, /* maxActiveTransactions= */ -1, t -> {});
    coordinator =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(
            delegate, registry, EXPIRATION_MILLIS);
  }

  private void forceExpire(String transactionId) {
    registry.get(transactionId).get().updateExpirationTime(0);
  }

  private void stubHeld(TwoPhaseCommitParticipant p, boolean held) throws TransactionException {
    when(p.hasTransactionContext(TX)).thenReturn(held);
  }

  @Test
  void sweep_WhenTransactionNotExpired_ShouldNotProbeOrReap() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), participant);

    coordinator.sweep();

    verify(participant, never()).hasTransactionContext(any());
    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void sweep_WhenExpiredWithoutParticipants_ShouldReap() throws Exception {
    // With no participant registered there is nothing to probe: the coordinator's expiration time
    // is authoritative (e.g. a transaction begun but never joined to any participant).
    coordinator.begin(null, false, Collections.emptyMap(), null);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void sweep_WhenParticipantStillHoldsTransaction_ShouldKeepUntilItIsGone() throws Exception {
    // The healthy long-running transaction: CRUD keeps the participant side alive while the
    // coordinator observes nothing. The probe must keep the entry, and only once the participant
    // reports the transaction gone may the reap happen.
    stubHeld(participant, true);
    coordinator.begin(null, false, Collections.emptyMap(), participant);

    forceExpire(TX);
    coordinator.sweep();
    verify(participant).hasTransactionContext(TX);
    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();

    // The alive verdict pushed the expiration time out, so another pass must not probe again.
    coordinator.sweep();
    verify(participant).hasTransactionContext(TX);

    // The participant no longer holds it (its own idle reaping released the context): the next
    // probe concludes the transaction is gone and releases the coordinator context.
    stubHeld(participant, false);
    forceExpire(TX);
    coordinator.sweep();
    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void sweep_WhenTransactionKept_ShouldRefreshRecencyForCapEviction() throws Exception {
    // A kept verdict must also mark the entry as recently used: a healthy long-running
    // transaction, whose only signal is participant-side CRUD, must not look cold under cap
    // pressure. Losing the touch would silently reintroduce that eviction bias.
    ActiveTransactionRegistry<TrackedTransaction> spiedRegistry =
        spy(
            new ActiveTransactionRegistry<TrackedTransaction>(
                /* expirationTimeMillis= */ -1, /* maxActiveTransactions= */ -1, t -> {}));
    ActiveTransactionManagedTwoPhaseCommitCoordinator kept =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(
            delegate, spiedRegistry, EXPIRATION_MILLIS);
    stubHeld(participant, true);
    kept.begin(null, false, Collections.emptyMap(), participant);
    spiedRegistry.get(TX).get().updateExpirationTime(0);

    kept.sweep();

    verify(spiedRegistry).touch(TX);
  }

  @Test
  void sweep_WhenAnyParticipantHoldsTransaction_ShouldKeep() throws Exception {
    // any-semantics: one participant already released it, the other still holds it. The
    // coordinator context stays: the transaction could still be driven for the present one.
    TwoPhaseCommitParticipant gone = mock(TwoPhaseCommitParticipant.class);
    when(gone.getId()).thenReturn("participant-2");
    stubHeld(gone, false);
    stubHeld(participant, true);
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    coordinator.registerParticipant(TX, gone);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void sweep_WhenProbeThrowsTransactionException_ShouldKeepFailOpen() throws Exception {
    // A probe failure must not reap a possibly-live transaction. The declared
    // TransactionException is the expected failure channel for a remote implementation - both for
    // an unreachable participant and for one that does not implement the probe yet.
    when(participant.hasTransactionContext(TX))
        .thenThrow(new TransactionException("unreachable", TX));
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void sweep_WhenProbeThrowsRuntimeException_ShouldKeepFailOpenAndFinishThePass() throws Exception {
    // An undeclared exception maps to the same fail-open answer as the declared one, and must not
    // abort the rest of the pass: another expired transaction in the same pass must still be
    // probed and reaped.
    when(participant.hasTransactionContext(TX)).thenThrow(new RuntimeException("boom"));
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    TwoPhaseCommitParticipant otherParticipant = mock(TwoPhaseCommitParticipant.class);
    when(otherParticipant.getId()).thenReturn("participant-2");
    when(delegate.begin(eq("tx-2"), eq(false), any(), any())).thenReturn("tx-2");
    coordinator.begin("tx-2", false, Collections.emptyMap(), otherParticipant);
    forceExpire(TX);
    forceExpire("tx-2");

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isPresent();
    verify(delegate).releaseTransactionContext("tx-2");
    assertThat(registry.get("tx-2")).isEmpty();
  }

  @Test
  void sweep_WhenReleaseOnWrappedThrows_ShouldStillRemoveEntryAndFinishThePass() throws Exception {
    // The guard around the reap's release, pinned as a design decision: a release that throws
    // neither aborts the pass nor keeps the entry for a retry. The entry is removed anyway -
    // retrying a release that keeps throwing would re-attempt (and WARN) every pass forever, so
    // a leaked wrapped context is left as the wrapped coordinator's responsibility - and other
    // expired transactions in the same pass are still reaped.
    doThrow(new RuntimeException("boom")).when(delegate).releaseTransactionContext(TX);
    stubHeld(participant, false);
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    TwoPhaseCommitParticipant otherParticipant = mock(TwoPhaseCommitParticipant.class);
    when(otherParticipant.getId()).thenReturn("participant-2");
    when(delegate.begin(eq("tx-2"), eq(false), any(), any())).thenReturn("tx-2");
    coordinator.begin("tx-2", false, Collections.emptyMap(), otherParticipant);
    forceExpire(TX);
    forceExpire("tx-2");

    assertThatCode(coordinator::sweep).doesNotThrowAnyException();

    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
    verify(delegate).releaseTransactionContext("tx-2");
    assertThat(registry.get("tx-2")).isEmpty();
  }

  @Test
  void sweep_WhenProbeThrowsError_ShouldAbortThePassAndKeepEntries() throws Exception {
    // An Error is deliberately not mapped to a liveness answer: it aborts the current pass. The
    // entries stay registered - nothing is reaped or leaked - and the scheduler wrapper is the
    // one to log it and retry on the next interval.
    when(participant.hasTransactionContext(TX)).thenThrow(new LinkageError("boom"));
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    assertThatThrownBy(coordinator::sweep).isInstanceOf(LinkageError.class);

    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void sweepSafely_WhenProbeThrowsError_ShouldNotPropagateAndKeepEntriesForRetry()
      throws Exception {
    // The scheduler's safety net: anything escaping a scheduled task suppresses every future run
    // silently, so the wrapper must swallow even an Error, leaving the entries registered for the
    // next interval's retry. Narrowing its catch to Exception would break exactly this.
    when(participant.hasTransactionContext(TX)).thenThrow(new LinkageError("boom"));
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    assertThatCode(coordinator::sweepSafely).doesNotThrowAnyException();
    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();

    // The entry is still expired (the aborted pass extended nothing), so the next pass retries
    // the probe; the participant now answers definitively and the transaction is reaped.
    doReturn(false).when(participant).hasTransactionContext(TX);
    coordinator.sweepSafely();
    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void sweep_WhenProbeThrowsNotFound_ShouldTreatAsAbsentAndReap() throws Exception {
    // TransactionNotFoundException is the probe contract's alternative carrier of "definitely no
    // context" - e.g. a remote probe rerouted to a successor node after a crash, answering via the
    // conventional not-found error mapping. It must lead to a reap, not a fail-open retention.
    when(participant.hasTransactionContext(TX))
        .thenThrow(new TransactionNotFoundException("no context on this node", TX));
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void sweep_WhenNotFoundAndPresentMix_ShouldKeep() throws Exception {
    // any-semantics: one participant's context is definitively gone (not-found carrier), the
    // other still holds it - the coordinator context stays.
    TwoPhaseCommitParticipant gone = mock(TwoPhaseCommitParticipant.class);
    when(gone.getId()).thenReturn("participant-2");
    when(gone.hasTransactionContext(TX))
        .thenThrow(new TransactionNotFoundException("no context on this node", TX));
    stubHeld(participant, true);
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    coordinator.registerParticipant(TX, gone);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void sweep_WhenReleaseThrowsNotFound_ShouldTreatAsAlreadyGoneAndFinishThePass() throws Exception {
    // TransactionNotFoundException from the wrapped release is the contract's alternative carrier
    // of "there was no context to release" - the outcome the reap wanted. The entry must still be
    // removed, and the pass must go on to reap the other expired transaction.
    doThrow(new TransactionNotFoundException("already gone", TX))
        .when(delegate)
        .releaseTransactionContext(TX);
    coordinator.begin(null, false, Collections.emptyMap(), null);
    when(delegate.begin(eq("tx-2"), eq(false), any(), any())).thenReturn("tx-2");
    coordinator.begin("tx-2", false, Collections.emptyMap(), null);
    forceExpire(TX);
    forceExpire("tx-2");

    coordinator.sweep();

    assertThat(registry.get(TX)).isEmpty();
    verify(delegate).releaseTransactionContext("tx-2");
    assertThat(registry.get("tx-2")).isEmpty();
  }

  @Test
  void sweep_WhenReleaseThrowsException_ShouldFinishThePass() throws Exception {
    // A failed release must not abort the rest of the pass: the guard logs and moves on, the
    // entry is still removed (the release is best-effort), and the other expired transaction is
    // still reaped.
    doThrow(new TransactionException("boom", TX)).when(delegate).releaseTransactionContext(TX);
    coordinator.begin(null, false, Collections.emptyMap(), null);
    when(delegate.begin(eq("tx-2"), eq(false), any(), any())).thenReturn("tx-2");
    coordinator.begin("tx-2", false, Collections.emptyMap(), null);
    forceExpire(TX);
    forceExpire("tx-2");

    coordinator.sweep();

    assertThat(registry.get(TX)).isEmpty();
    verify(delegate).releaseTransactionContext("tx-2");
    assertThat(registry.get("tx-2")).isEmpty();
  }

  @Test
  void sweep_WhenRegistrationLandsDuringProbe_ShouldNotReap() throws Exception {
    // The reap-vs-registration race: a registration lands while the probe is in flight. It pushes
    // the expiration time out under the entry monitor before delegating, so the reap re-check
    // must back off even though every probed participant answered absent.
    TwoPhaseCommitParticipant late = mock(TwoPhaseCommitParticipant.class);
    when(late.getId()).thenReturn("participant-2");
    when(participant.hasTransactionContext(TX))
        .thenAnswer(
            invocation -> {
              coordinator.registerParticipant(TX, late);
              return false;
            });
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
    assertThat(registry.get(TX).get().hasParticipant("participant-2")).isTrue();
  }

  @Test
  void sweep_WhenFirstRegistrationJoinOutlivesExpiration_ShouldProbeNotReap() throws Exception {
    // A first registration whose join outlives the expiration period. The participant has joined -
    // the client legitimately owns the transaction on it - but the delegated join call has not
    // returned yet, so the participant would be invisible to the sweep if it were published only
    // after the join. It is published before the join instead, so the sweep probes the live
    // participant and keeps the transaction, rather than taking the no-participants fast path and
    // reaping it on the wall clock alone.
    stubHeld(participant, true);
    coordinator.begin(null, false, Collections.emptyMap(), null);

    CountDownLatch joinStarted = new CountDownLatch(1);
    CountDownLatch releaseJoin = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              joinStarted.countDown();
              releaseJoin.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(delegate)
        .registerParticipant(eq(TX), any());

    Thread registration =
        new Thread(
            () -> {
              try {
                coordinator.registerParticipant(TX, participant);
              } catch (TransactionException e) {
                throw new AssertionError(e);
              }
            });
    registration.start();
    try {
      // Inside the join: the participant is already published, but the join has not returned.
      assertThat(joinStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // The join outlives the expiration period.
      forceExpire(TX);
      coordinator.sweep();
    } finally {
      releaseJoin.countDown();
      registration.join(TimeUnit.SECONDS.toMillis(5));
    }

    // The sweep probed the live participant and kept the transaction; it did not reap.
    verify(participant).hasTransactionContext(TX);
    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void registerParticipant_ShouldDelegateAndTrackParticipant() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.registerParticipant(TX, participant);

    verify(delegate).registerParticipant(TX, participant);
    assertThat(registry.get(TX).get().hasParticipant("participant-1")).isTrue();
  }

  @Test
  void registerParticipant_ShouldExtendExpiration() throws Exception {
    // A registration is coordinator-observable activity: it pushes the expiration time a full
    // period out, so the next pass must not probe.
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.registerParticipant(TX, participant);
    coordinator.sweep();

    verify(participant, never()).hasTransactionContext(any());
    verify(delegate, never()).releaseTransactionContext(any());
    assertThat(registry.get(TX)).isPresent();
  }

  @Test
  void registerParticipant_WhenNotTracked_ShouldDelegateWithoutTracking() throws Exception {
    // No tracked entry means the transaction already hit a terminal step or was reaped - the
    // wrapped coordinator is authoritative and rejects the registration (its context is released
    // on those paths). Nothing may be recreated on this path.
    doThrow(new TransactionNotFoundException("no context", TX))
        .when(delegate)
        .registerParticipant(TX, participant);

    try {
      coordinator.registerParticipant(TX, participant);
    } catch (TransactionNotFoundException ignored) {
      // expected
    }

    verify(delegate).registerParticipant(TX, participant);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void registerParticipant_WhenDelegateThrows_ShouldStillTrackButReapWhenProbeAnswersAbsent()
      throws Exception {
    // The participant is published before the join is delegated, so a sweep firing while a slow
    // join is in flight probes it instead of reaping on the wall clock alone. The consequence is
    // that a failed join leaves the participant tracked - accepted, and benign: a participant that
    // never joined answers false to a probe, so the reap still proceeds, and a failed registration
    // does not keep a dead transaction alive forever. (Only an unreachable participant pins the
    // entry, which is the fail-open retention used everywhere else.)
    coordinator.begin(null, false, Collections.emptyMap(), null);
    doThrow(new TransactionException("boom", TX))
        .when(delegate)
        .registerParticipant(TX, participant);

    try {
      coordinator.registerParticipant(TX, participant);
    } catch (TransactionException ignored) {
      // expected
    }

    assertThat(registry.get(TX).get().hasParticipant("participant-1")).isTrue();

    // The never-joined participant reports no context, so the sweep reaps rather than keeping the
    // transaction alive on the strength of a participant that never joined.
    stubHeld(participant, false);
    forceExpire(TX);
    coordinator.sweep();
    verify(delegate).releaseTransactionContext(TX);
    assertThat(registry.get(TX)).isEmpty();
  }

  @Test
  void registerParticipant_WithDuplicateParticipantId_ShouldKeepFirstInstance() throws Exception {
    // First-wins per participant ID, mirroring the wrapped coordinator's idempotent registration:
    // the wrapped side joined the first instance, so that is the instance the probe must use.
    TwoPhaseCommitParticipant second = mock(TwoPhaseCommitParticipant.class);
    when(second.getId()).thenReturn("participant-1");
    stubHeld(participant, false);
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.registerParticipant(TX, participant);
    coordinator.registerParticipant(TX, second);

    forceExpire(TX);
    coordinator.sweep();
    verify(participant).hasTransactionContext(TX);
    verify(second, never()).hasTransactionContext(any());
  }

  @Test
  void commit_ShouldStopTracking() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.commit(TX);

    verify(delegate).commit(TX);
    // The transaction is no longer tracked, so the sweep must not release its context.
    assertThat(registry.get(TX)).isEmpty();
    coordinator.sweep();
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void rollback_ShouldStopTracking() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.rollback(TX);

    verify(delegate).rollback(TX);
    assertThat(registry.get(TX)).isEmpty();
    coordinator.sweep();
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void commit_WhenDelegateThrows_ShouldStillStopTracking() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);
    doThrow(new CommitException("boom", TX)).when(delegate).commit(TX);

    try {
      coordinator.commit(TX);
    } catch (CommitException ignored) {
      // expected
    }

    // The finally block deregistered the transaction even though commit threw, so the sweep must
    // not subsequently release its context (no double reap).
    assertThat(registry.get(TX)).isEmpty();
    coordinator.sweep();
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void rollback_WhenDelegateThrows_ShouldStillStopTracking() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);
    doThrow(new RollbackException("boom", TX)).when(delegate).rollback(TX);

    try {
      coordinator.rollback(TX);
    } catch (RollbackException ignored) {
      // expected
    }

    assertThat(registry.get(TX)).isEmpty();
    coordinator.sweep();
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void releaseTransactionContext_ShouldDelegateOnceAndStopTracking() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.releaseTransactionContext(TX);

    // The sweep must not call releaseTransactionContext again after the client already released it.
    assertThat(registry.get(TX)).isEmpty();
    coordinator.sweep();
    verify(delegate).releaseTransactionContext(TX);
  }

  @Test
  void begin_WhenDelegateThrows_ShouldNotTrack() throws Exception {
    doThrow(new TransactionException("boom", TX))
        .when(delegate)
        .begin(any(), eq(true), any(), any());

    try {
      coordinator.begin(null, true, Collections.emptyMap(), null);
    } catch (TransactionException ignored) {
      // expected
    }

    // A failed begin tracked nothing, so the sweep must not release any context.
    coordinator.sweep();
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void sweep_AfterClose_ShouldNotProbeOrRelease() throws Exception {
    stubHeld(participant, true);
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.close();
    coordinator.sweep();

    verify(delegate).close();
    // A pass overlapping the shutdown must not probe or release on behalf of a closed
    // coordinator: the wrapped close discards the contexts.
    verify(participant, never()).hasTransactionContext(any());
    verify(delegate, never()).releaseTransactionContext(any());
  }

  @Test
  void sweep_WhenCloseLandsDuringProbe_ShouldNotRelease() throws Exception {
    // close() racing an in-flight probe: the per-entry closed check in sweep() passed before the
    // probe's I/O began, so the re-check after the probe returns is what must stop the reap - a
    // post-close pass must not release contexts on behalf of a closed coordinator.
    when(participant.hasTransactionContext(TX))
        .thenAnswer(
            invocation -> {
              coordinator.close();
              return false;
            });
    coordinator.begin(null, false, Collections.emptyMap(), participant);
    forceExpire(TX);

    coordinator.sweep();

    verify(delegate, never()).releaseTransactionContext(any());
    verify(delegate).close();
  }

  @Test
  void backgroundSweeping_ShouldReapExpiredTransactionWhoseParticipantIsGone() throws Exception {
    // The one wall-clock test: pins that the public constructor actually schedules the sweep.
    // Everything behavioral is covered deterministically above. The unstubbed probe answers
    // false, so the first pass after the expiration reaps.
    ActiveTransactionManagedTwoPhaseCommitCoordinator scheduled =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(
            delegate, /* expirationTimeMillis= */ 100, /* maxActiveTransactions= */ -1);
    try {
      scheduled.begin(null, false, Collections.emptyMap(), participant);
      verify(delegate, timeout(10000)).releaseTransactionContext(TX);
    } finally {
      scheduled.close();
    }
  }

  @Test
  void eviction_ShouldReleaseContextUnconditionallyWithoutProbing() throws Exception {
    // Cap pressure means memory must be freed: eviction releases without consulting the probe.
    // Local mocks keep this test independent of the shared setUp stubbing.
    TwoPhaseCommitCoordinator localDelegate = mock(TwoPhaseCommitCoordinator.class);
    when(localDelegate.begin(any(), eq(false), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    CountDownLatch released = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              released.countDown();
              return null;
            })
        .when(localDelegate)
        .releaseTransactionContext(any());
    ActiveTransactionManagedTwoPhaseCommitCoordinator capped =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(
            localDelegate, /* expirationTimeMillis= */ -1, /* maxActiveTransactions= */ 1);
    try {
      capped.begin("tx-a", false, Collections.emptyMap(), participant);
      capped.begin("tx-b", false, Collections.emptyMap(), participant);

      // Caffeine decides size-based eviction during maintenance, and maintenance is re-triggered
      // by cache writes; two racing writes can leave the cache over capacity but quiescent, with
      // the decision deferred until the next write (in production, ongoing traffic provides it
      // constantly). Poke with further begins - each is a real write and itself over-capacity
      // pressure - while polling, so the eviction decision is reliably driven.
      long deadlineMillis = System.currentTimeMillis() + 10000;
      int poke = 0;
      while (released.getCount() > 0 && System.currentTimeMillis() < deadlineMillis) {
        capped.begin("tx-poke-" + poke++, false, Collections.emptyMap(), null);
        TimeUnit.MILLISECONDS.sleep(50);
      }

      verify(localDelegate, atLeastOnce()).releaseTransactionContext(any());
      verify(participant, never()).hasTransactionContext(any());
    } finally {
      capped.close();
    }
  }
}
