package com.scalar.db.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ActiveTransactionManagedTwoPhaseCommitCoordinatorTest {

  private static final String TX = "tx-1";

  // A short expiration so reaping fires quickly; the registry sweeps roughly every second.
  private static final long EXPIRATION_MILLIS = 100;

  // Long enough to cover at least one post-expiration sweep, used to assert reaping does NOT
  // happen.
  private static final long PAST_SWEEP_MILLIS = 1500;

  @Mock private TwoPhaseCommit.Coordinator delegate;
  @Mock private TwoPhaseCommit.Participant participant;
  private ActiveTransactionManagedTwoPhaseCommitCoordinator coordinator;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(delegate.begin(any(), eq(false), any(), any())).thenReturn(TX);
    coordinator =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(delegate, EXPIRATION_MILLIS);
  }

  @Test
  void begin_ShouldDelegateAndRegister_ThenReapReleasesContextOnDelegate() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    verify(delegate).begin(null, false, Collections.emptyMap(), null);
    // After the idle expiration, the reaper releases the context on the wrapped coordinator.
    verify(delegate, timeout(PAST_SWEEP_MILLIS * 4)).releaseContext(TX);
  }

  @Test
  void registerParticipant_ShouldDelegate() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.registerParticipant(TX, participant);

    verify(delegate).registerParticipant(TX, participant);
  }

  @Test
  void commit_ShouldStopReaping() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.commit(TX);

    verify(delegate).commit(TX);
    // The transaction is no longer tracked, so the reaper must not release its context.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseContext(TX);
  }

  @Test
  void rollback_ShouldStopReaping() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.rollback(TX);

    verify(delegate).rollback(TX);
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseContext(TX);
  }

  @Test
  void commit_WhenDelegateThrows_ShouldStillStopReaping() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);
    doThrow(new CommitException("boom", TX)).when(delegate).commit(TX);

    try {
      coordinator.commit(TX);
    } catch (CommitException ignored) {
      // expected
    }

    // The finally block deregistered the transaction even though commit threw, so the reaper must
    // not subsequently release its context (no double reap).
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseContext(TX);
  }

  @Test
  void rollback_WhenDelegateThrows_ShouldStillStopReaping() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);
    doThrow(new RollbackException("boom", TX)).when(delegate).rollback(TX);

    try {
      coordinator.rollback(TX);
    } catch (RollbackException ignored) {
      // expected
    }

    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseContext(TX);
  }

  @Test
  void releaseContext_ShouldDelegateOnceAndStopReaping() throws Exception {
    coordinator.begin(null, false, Collections.emptyMap(), null);

    coordinator.releaseContext(TX);

    // The reaper must not call releaseContext again after the client already released it.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate).releaseContext(TX);
  }

  @Test
  void begin_WhenDelegateThrows_ShouldNotRegister() throws Exception {
    doThrow(new TransactionException("boom", TX))
        .when(delegate)
        .begin(any(), eq(true), any(), any());

    try {
      coordinator.begin(null, true, Collections.emptyMap(), null);
    } catch (TransactionException ignored) {
      // expected
    }

    // A failed begin registered nothing, so the reaper must not release any context.
    Thread.sleep(PAST_SWEEP_MILLIS);
    verify(delegate, never()).releaseContext(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void begin_ShouldRegister_AndRegisterParticipant_ShouldRefreshIdleTimer() throws Exception {
    // Deterministic wiring check (no Thread.sleep): begin registers the transaction and
    // registerParticipant refreshes the idle timer via registry.touch(id) — the only
    // coordinator-side call that extends a transaction's life. A mock registry verifies these
    // directly, so a dropped touch() (which would let an active transaction be reaped) is caught
    // immediately. The registerParticipant_ShouldDelegate test asserts delegation only and stays
    // green without it.

    ActiveTransactionRegistry<String> registry = mock(ActiveTransactionRegistry.class);
    ActiveTransactionManagedTwoPhaseCommitCoordinator c =
        new ActiveTransactionManagedTwoPhaseCommitCoordinator(delegate, registry);

    c.begin(null, false, Collections.emptyMap(), null); // delegate.begin is stubbed to return TX
    c.registerParticipant(TX, participant);

    verify(registry).add(TX, TX); // begin tracks the transaction
    verify(registry).touch(TX); // registerParticipant refreshes the idle timer
  }
}
