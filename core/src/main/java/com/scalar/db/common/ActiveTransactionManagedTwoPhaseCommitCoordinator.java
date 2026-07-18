package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TwoPhaseCommitCoordinator} decorator that reaps the contexts of transactions whose
 * participants no longer hold them.
 *
 * <p>Each transaction is tracked from {@link #begin} until its terminal step ({@link #commit} /
 * {@link #rollback} / {@link #releaseTransactionContext}), with a per-transaction expiration time
 * that {@code begin} and {@link #joinParticipant} push {@code expirationTimeMillis} out. The
 * coordinator observes only those two calls — the CRUD a transaction issues goes directly to its
 * participants — so an elapsed expiration time alone cannot tell an abandoned transaction from a
 * healthy long-running one. A background sweep therefore probes the joined participants of every
 * expired transaction ({@link TwoPhaseCommitParticipant#hasTransactionContext}) — in place, while
 * the transaction stays tracked:
 *
 * <ul>
 *   <li>If any participant still holds the transaction — or cannot be probed — the expiration time
 *       is pushed another {@code expirationTimeMillis} out and the transaction lives on. The
 *       coordinator context is useful exactly as long as some participant context survives, so this
 *       errs on the side of retention.
 *   <li>If no participant holds it, the transaction can no longer commit (the participants' own
 *       idle reaping has already released their contexts), so the sweep calls {@link
 *       TwoPhaseCommitCoordinator#releaseTransactionContext} on the wrapped coordinator to free its
 *       role-local resources without any storage rollback. A subsequent {@code commit} for a reaped
 *       transaction fails with {@link TransactionNotFoundException} (retriable from the client's
 *       perspective), while a subsequent {@code rollback} is a benign no-op; either way the records
 *       left behind are recovered lazily by the usual recovery path (as an abort).
 * </ul>
 *
 * <p>{@code expirationTimeMillis} is therefore the <em>probing period</em>, not a
 * transaction-lifetime bound: a context lives exactly as long as some participant holds the
 * transaction, or its absence cannot be confirmed. Fail-open probing keeps a context alive with no
 * time limit, and each such retention logs a WARN, so a persistent probe failure is loudly visible.
 * During a total outage of a participant cluster no new transactions can join it, so the affected
 * entries are bounded to the in-flight snapshot at outage time; every cycle re-probes, so they are
 * reaped as soon as probes are answered again; and the registry cap stays the hard memory bound. A
 * participant cluster that is <em>permanently</em> unreachable would keep its snapshot probing
 * forever, cleared only when the coordinator process is restarted.
 *
 * <p>One boundary of that semantics: a transaction with no joined participants yet (begun but not
 * joined to anything) has nothing to probe, so its expiration time is authoritative and the reap
 * rests on the wall clock alone. {@link #joinParticipant} publishes the participant into the
 * tracked entry <em>before</em> delegating the join, so this fast path fires only when no join has
 * reached that point — never while a join is in flight, whose participant is already visible to
 * probe (the delegated join can hold the wrapped coordinator's per-context monitor across remote
 * I/O and outlast a period, so it must not be reapable on the wall clock alone). Probe-before-reap
 * is what makes the sweep tolerant of clock jumps everywhere else; in the begin-to-first-join
 * window a forward jump exceeding one period can reap a healthy just-begun transaction, whose next
 * step then fails with the same retriable {@link TransactionNotFoundException} — accepted, as the
 * window is brief and a monotonic-clock scheme was judged not worth the complexity.
 *
 * <p>The sweep runs every second (or every {@code expirationTimeMillis}, if that is shorter). A
 * pass only reads each entry's expiration time, and a kept transaction is re-probed only after
 * another full period, so the short interval buys prompt expiry detection without inflating the
 * probe rate.
 *
 * <p>Because the probe happens while the transaction stays tracked, keeping a transaction is a
 * deadline extension and reaping one is a plain removal — an entry never has to be re-registered
 * after leaving the registry. Idle expiration on the underlying registry is disabled accordingly:
 * the registry contributes only the size cap, whose eviction remains an unconditional release
 * (extension is not an option when memory must be freed).
 *
 * <p>This decorator assumes transaction IDs are not reused: a finished transaction's ID is never
 * passed to {@link #begin} again. Under that assumption a tracked entry belongs to exactly one
 * transaction for its whole life, which keeps the concurrency reasoning below small. It is intended
 * to be the outermost coordinator decorator so that the reap traverses the inner decorators via
 * {@code releaseTransactionContext}.
 *
 * <p>Thread safety: the {@link ThreadSafe} guarantee relies on two mechanisms. First, the wrapped
 * coordinator honors the {@link TwoPhaseCommitCoordinator} concurrency contract — it serializes its
 * own per-transaction work, including {@code releaseTransactionContext} concurrently with any other
 * method for the same transaction ID (e.g. {@code ConsensusCommitCoordinator} synchronizes every
 * per-transaction method on a per-context monitor) — which makes the sweep's {@code
 * releaseTransactionContext} safe against an in-flight {@code commit}/{@code rollback}. Second, the
 * sweep and {@link #joinParticipant} shake hands on the tracked entry's monitor: a join pushes the
 * expiration time out under the monitor <em>before</em> delegating to the wrapped coordinator, and
 * the sweep re-checks under the same monitor that the expiration time has not moved before
 * releasing and removing. A reap therefore never overlaps a join that is about to succeed — either
 * the join extends the deadline first and the sweep backs off, or the reap completes first and the
 * delegated join is rejected by the wrapped coordinator, whose context is already released. Probing
 * itself is I/O and runs outside the monitor, so a slow probe never blocks a join.
 *
 * <p>A reap whose release lands behind an in-flight terminal step for the same transaction blocks
 * on the wrapped coordinator's serialization — on the sweeper thread, under the entry monitor —
 * stalling the rest of the pass until the step completes. The lock ordering admits no deadlock, and
 * the unblocked release then finds the context already gone and is a no-op, so the price is
 * reap-detection latency for the remaining expired transactions, never correctness.
 */
@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitCoordinator
    extends DecoratedTwoPhaseCommitCoordinator {

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedTwoPhaseCommitCoordinator.class);

  private static final long SWEEP_INTERVAL_MILLIS = 1000;

  private final ActiveTransactionRegistry<TrackedTransaction> registry;
  private final long expirationTimeMillis;
  @Nullable private final ScheduledExecutorService sweeper;

  // Set once by close(); the sweep checks it so a pass overlapping the shutdown stops releasing
  // contexts the wrapped close is about to discard anyway.
  private volatile boolean closed;

  public ActiveTransactionManagedTwoPhaseCommitCoordinator(
      TwoPhaseCommitCoordinator coordinator, long expirationTimeMillis, int maxActiveTransactions) {
    this(
        coordinator,
        newCapOnlyRegistry(coordinator, maxActiveTransactions),
        expirationTimeMillis,
        /* scheduleSweeps= */ true);
  }

  @VisibleForTesting
  ActiveTransactionManagedTwoPhaseCommitCoordinator(
      TwoPhaseCommitCoordinator coordinator,
      ActiveTransactionRegistry<TrackedTransaction> registry,
      long expirationTimeMillis) {
    this(coordinator, registry, expirationTimeMillis, /* scheduleSweeps= */ false);
  }

  // FutureReturnValueIgnored: the ScheduledFuture of the sweep schedule is deliberately unused.
  // Its exception-reporting role is void — sweepSafely lets nothing escape, precisely so the
  // schedule can never be silently cancelled — and its cancellation role is covered by close()'s
  // shutdownNow(), which also interrupts an in-flight probe.
  @SuppressWarnings("FutureReturnValueIgnored")
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  private ActiveTransactionManagedTwoPhaseCommitCoordinator(
      TwoPhaseCommitCoordinator coordinator,
      ActiveTransactionRegistry<TrackedTransaction> registry,
      long expirationTimeMillis,
      boolean scheduleSweeps) {
    super(coordinator);
    this.registry = registry;
    this.expirationTimeMillis = expirationTimeMillis;
    if (scheduleSweeps && expirationTimeMillis > 0) {
      // The this::sweepSafely reference escapes the constructor, but every field the sweep reads
      // (registry, expirationTimeMillis; closed relies only on its default) is assigned above this
      // call, and the executor's submission happens-before guarantee publishes those writes to the
      // sweeper thread, so the escape is safe regardless of when the first run fires.
      long sweepIntervalMillis = Math.min(expirationTimeMillis, SWEEP_INTERVAL_MILLIS);
      sweeper = newSweeperExecutor();
      sweeper.scheduleWithFixedDelay(
          this::sweepSafely, sweepIntervalMillis, sweepIntervalMillis, TimeUnit.MILLISECONDS);
    } else {
      // A non-positive expiration time disables the liveness management, mirroring how a
      // non-positive lifetime disables idle expiration elsewhere; only the size cap remains.
      sweeper = null;
    }
  }

  private static ActiveTransactionRegistry<TrackedTransaction> newCapOnlyRegistry(
      TwoPhaseCommitCoordinator coordinator, int maxActiveTransactions) {
    // Idle expiration is disabled (non-positive lifetime): liveness is owned by the sweep, which
    // probes expired entries in place, so an entry only ever leaves the registry through a terminal
    // step, a reap, or a cap eviction.
    return new ActiveTransactionRegistry<>(
        /* expirationTimeMillis= */ -1,
        maxActiveTransactions,
        tracked -> coordinator.releaseTransactionContext(tracked.transactionId));
  }

  private static ScheduledExecutorService newSweeperExecutor() {
    ThreadFactory threadFactory =
        r -> {
          Thread thread = new Thread(r, "two-phase-commit-coordinator-sweeper");
          thread.setDaemon(true);
          return thread;
        };
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  // The one rule for how far every deadline reaches - initial tracking, a join, and a kept-alive
  // verdict all push the expiration time one period out from now.
  private long nextExpirationTimeMillis() {
    return System.currentTimeMillis() + expirationTimeMillis;
  }

  @Override
  public String begin(
      @Nullable String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    String id = super.begin(transactionId, readOnly, attributes);
    TrackedTransaction tracked = new TrackedTransaction(id, nextExpirationTimeMillis());
    // The slot is necessarily free — transaction IDs are not reused (see the class Javadoc) and
    // the terminal steps always deregister — and no sweep can be deciding about this ID, because
    // a sweep only sees registered entries. Tracking therefore needs no handshake with the sweep.
    registry.add(id, tracked);
    return id;
  }

  @Override
  public void joinParticipant(String transactionId, TwoPhaseCommitParticipant participant)
      throws TransactionException {
    // get() marks the entry as recently used, so an actively-joining transaction is
    // preferentially retained under cap pressure.
    Optional<TrackedTransaction> current = registry.get(transactionId);
    if (!current.isPresent()) {
      // Not tracked: a terminal step or a reap already deregistered the transaction (either way
      // the wrapped context is released, so the delegated join is rejected), or a cap eviction
      // is releasing it right now (the join may slip through, but the transaction is doomed
      // regardless). Delegate for the authoritative answer; there is nothing worth tracking.
      super.joinParticipant(transactionId, participant);
      return;
    }
    TrackedTransaction tracked = current.get();
    // Push the expiration time out and publish the participant BEFORE delegating (see the class
    // Javadoc). The sweep re-checks the expiration time under the entry monitor before reaping, so
    // a reap never overlaps a join that is about to succeed; and publishing the participant first
    // means a sweep firing while the (potentially remote, potentially slow) join is in flight
    // probes it instead of taking the no-participants fast path and reaping on the wall clock
    // alone.
    // First-wins per participant ID, mirroring the wrapped coordinator's idempotent join:
    // instances joined under the same participant ID front the same stores, so they are
    // interchangeable for probing. A failed join leaves the participant tracked, which is benign: a
    // participant that never joined answers false to a probe, so the reap still proceeds; only an
    // unreachable one pins the entry, the fail-open retention used everywhere else.
    tracked.updateExpirationTime(nextExpirationTimeMillis());
    tracked.addParticipant(participant);
    super.joinParticipant(transactionId, participant);
  }

  @Override
  public void commit(String transactionId)
      throws CommitException, UnknownTransactionStatusException, TransactionNotFoundException {
    try {
      super.commit(transactionId);
    } finally {
      // Remove on every outcome, including CommitException / UnknownTransactionStatusException: the
      // coordinator's durable state record (not this in-memory entry) is the source of truth for
      // recovery, so keeping the entry would only produce a spurious reap log and a no-op
      // releaseTransactionContext.
      registry.remove(transactionId);
    }
  }

  @Override
  public void rollback(String transactionId) throws RollbackException {
    try {
      super.rollback(transactionId);
    } finally {
      registry.remove(transactionId);
    }
  }

  @Override
  public void releaseTransactionContext(String transactionId) {
    try {
      super.releaseTransactionContext(transactionId);
    } finally {
      registry.remove(transactionId);
    }
  }

  @Override
  public void close() {
    // Stop probing first: set the flag an in-flight sweep checks, cancel the schedule, and
    // interrupt a probe blocked on I/O. The entries still registered need no draining — with idle
    // expiration disabled the registry does nothing on its own, no further sweeps run, and the
    // wrapped close below discards the contexts.
    closed = true;
    if (sweeper != null) {
      sweeper.shutdownNow();
    }
    super.close();
  }

  @VisibleForTesting
  void sweepSafely() {
    try {
      sweep();
    } catch (Throwable t) {
      // Deliberately Throwable, and load-bearing: if anything escapes a scheduled task,
      // scheduleWithFixedDelay suppresses every future run and captures the throwable in the
      // never-read future — probing would end permanently and silently. Catching everything here
      // keeps the schedule alive and the failure visible; the tracked entries are untouched, so
      // the next pass simply retries.
      logger.warn("Failed to sweep the tracked transactions", t);
    }
  }

  /**
   * One probing pass: probes, in place, every expired transaction, and reaps the ones whose absence
   * is confirmed. Invoked on the sweeper thread; tests invoke it directly.
   */
  @VisibleForTesting
  void sweep() {
    registry.forEach(
        (transactionId, tracked) -> {
          if (closed) {
            return;
          }
          long observedExpirationTimeMillis = tracked.expirationTimeMillis;
          if (System.currentTimeMillis() < observedExpirationTimeMillis) {
            return;
          }
          probeAndDecide(transactionId, tracked, observedExpirationTimeMillis);
        });
  }

  private void probeAndDecide(
      String transactionId, TrackedTransaction tracked, long observedExpirationTimeMillis) {
    if (tracked.participants.isEmpty()) {
      // Nothing to probe: the coordinator is the only observation point, so its expiration time
      // is authoritative (e.g. a transaction begun but never joined to any participant).
      tracked.reapUnlessExtended(
          observedExpirationTimeMillis, () -> reap(transactionId, "has no joined participants"));
      return;
    }
    // Probing is (potentially remote) I/O, so it runs outside the entry monitor: a slow probe
    // never blocks a join.
    String aliveReason = probe(transactionId, tracked);
    if (closed) {
      // close() may have run while the probe was in flight - the per-entry check in sweep()
      // passed before the I/O began. Deciding on a stale answer here could release a context on
      // behalf of a closed coordinator, so hands off; the wrapped close discards the contexts.
      return;
    }
    if (aliveReason == null) {
      tracked.reapUnlessExtended(
          observedExpirationTimeMillis,
          () -> reap(transactionId, "no participant holds it anymore"));
      return;
    }
    logger.debug(
        "The transaction is expired but kept because {}. Transaction ID: {}",
        aliveReason,
        transactionId);
    // Fresh evidence of liveness: the next probe is one expiration period out, and the entry
    // counts as recently used for cap eviction — a healthy long-running transaction, whose only
    // signal is participant-side CRUD, should not look cold under cap pressure.
    tracked.updateExpirationTime(nextExpirationTimeMillis());
    registry.touch(transactionId);
  }

  private void reap(String transactionId, String reason) {
    logger.warn(
        "The transaction is expired and {}; releasing the context. Transaction ID: {}",
        reason,
        transactionId);
    // Release before removing: a racing joinParticipant that finds the entry already gone
    // delegates straight to the wrapped coordinator, and only a completed release guarantees the
    // wrapped coordinator rejects that join instead of accepting it onto a context this reap
    // is destroying. releaseTransactionContext is a pure in-memory reap that no-ops on an
    // unknown transaction and does not throw — trusted like the removal below — so it is not
    // guarded per entry; any unexpected failure is caught by sweepSafely and retried on the next
    // pass.
    super.releaseTransactionContext(transactionId);
    registry.remove(transactionId);
  }

  /**
   * Probes every joined participant and returns why the transaction counts as alive, or {@code
   * null} if every participant definitely no longer holds it. Any single positive answer is
   * conclusive. A {@link TransactionNotFoundException} is a definitive "no context" (the probe
   * contract's alternative carrier of {@code false}); any other failure is mapped per participant
   * to "possibly present", keeping the probe fail-open.
   */
  @Nullable
  private String probe(String transactionId, TrackedTransaction tracked) {
    String aliveReason = null;
    for (TwoPhaseCommitParticipant participant : tracked.participants.values()) {
      boolean held;
      try {
        held = participant.hasTransactionContext(transactionId);
      } catch (TransactionNotFoundException e) {
        // A definitive "no context" carried on the conventional not-found channel instead of a
        // false return — e.g. a remote participant whose node crashed, with the probe rerouted to
        // a successor node that never held the context. Treated exactly like held == false, per
        // the probe's contract.
        held = false;
      } catch (Exception e) {
        // Fail-open: a probe failure — the declared TransactionException from an unreachable (or
        // not-yet-probe-capable) remote participant being the expected case — must not reap a
        // possibly-live transaction. Wrong retentions of dead transactions are deliberately
        // unbounded in time (see the class Javadoc for why that is acceptable) and bounded in
        // count by cap eviction. An Error deliberately propagates instead of being mapped to a
        // liveness answer: it aborts only the current pass — the entries stay registered, and the
        // sweep scheduler's catch-all logs it and retries on the next interval.
        logger.warn(
            "Probing participant {} for the expired transaction failed; treating the transaction "
                + "as still held. Transaction ID: {}",
            participant.getId(),
            transactionId,
            e);
        aliveReason = "probing participant " + participant.getId() + " failed";
        continue;
      }
      if (held) {
        return "participant " + participant.getId() + " reported it present";
      }
    }
    return aliveReason;
  }

  /**
   * A registry entry tracking one transaction: the participants to probe once expired, and the
   * expiration time. It lives and dies with the registry entry, so every removal cause — terminal
   * steps, reap, cap eviction — releases the participant references with it.
   */
  @VisibleForTesting
  static final class TrackedTransaction {
    private final String transactionId;

    // Keyed by TwoPhaseCommitParticipant#getId (first-wins, mirroring the wrapped coordinator's
    // idempotent join). Written by begin/joinParticipant threads and read by the sweeper, hence
    // concurrent.
    private final ConcurrentMap<String, TwoPhaseCommitParticipant> participants =
        new ConcurrentHashMap<>();

    // The absolute wall-clock time at which the transaction becomes a probe candidate. Pushed out
    // by begin/joinParticipant and by a sweep that found (or failed to rule out) a participant
    // still holding the transaction. Written under the entry monitor (the join-vs-reap
    // handshake, see the class Javadoc); volatile so the sweep's cheap pre-check can read it
    // without the monitor.
    private volatile long expirationTimeMillis;

    TrackedTransaction(String transactionId, long expirationTimeMillis) {
      this.transactionId = transactionId;
      this.expirationTimeMillis = expirationTimeMillis;
    }

    synchronized void updateExpirationTime(long expirationTimeMillis) {
      this.expirationTimeMillis = expirationTimeMillis;
    }

    /**
     * Runs {@code reap} under the entry monitor, unless the expiration time has moved since the
     * sweep observed it — the sweep's side of the join-vs-reap handshake (see the class Javadoc):
     * {@link #updateExpirationTime} takes the same monitor, so a join either extends the expiration
     * time first (and the reap backs off here) or blocks until the reap — including the release it
     * performs — has completed.
     */
    synchronized void reapUnlessExtended(long observedExpirationTimeMillis, Runnable reap) {
      if (expirationTimeMillis != observedExpirationTimeMillis) {
        // The expiration time moved while the sweep was deciding: a join pushed it out
        // under this monitor before delegating, so the transaction just proved itself alive.
        return;
      }
      reap.run();
    }

    void addParticipant(TwoPhaseCommitParticipant participant) {
      participants.putIfAbsent(participant.getId(), participant);
    }

    @VisibleForTesting
    boolean hasParticipant(String participantId) {
      return participants.containsKey(participantId);
    }
  }
}
