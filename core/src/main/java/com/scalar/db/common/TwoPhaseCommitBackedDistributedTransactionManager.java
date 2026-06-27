package com.scalar.db.common;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Adapts a {@link TwoPhaseCommit.Coordinator} + a single in-process {@link
 * TwoPhaseCommit.Participant} to the {@link com.scalar.db.api.DistributedTransactionManager} API.
 *
 * <p>Each {@code begin} allocates a transaction via the coordinator, registers the in-process
 * participant, and returns a {@link TwoPhaseCommitBackedDistributedTransaction}. The full two-phase
 * commit protocol is internalized behind the single-phase {@code commit()} — so this manager can
 * front the new Coordinator/Participant pair for (a) single-cluster clients and (b) running the
 * existing {@code DistributedTransaction} integration-test corpus against the new code path.
 *
 * <p>Implementation-agnostic: it depends only on the public {@link TwoPhaseCommit} and {@link
 * com.scalar.db.api.DistributedTransactionManager} interfaces. The concrete coordinator and
 * participant (e.g., the consensuscommit-backed ones) are injected by the wiring layer.
 *
 * <p>Limitations: the {@link TwoPhaseCommit.Coordinator} interface exposes no by-ID operation on
 * the Coordinator state table, so {@link #getState(String)}, {@link #rollback(String)} (and {@code
 * abort(String)}, which delegates to it), {@link #finishTransaction(String)}, {@code
 * recoverRecord}, and {@code resume} are unsupported and throw {@link
 * UnsupportedOperationException}. The deprecated {@code start(...)} overloads ignore the
 * per-transaction isolation / serializable strategy they carry; per-transaction isolation is
 * instead honored via the {@code cc-transaction-isolation} attribute passed to {@code begin}.
 */
public class TwoPhaseCommitBackedDistributedTransactionManager
    extends AbstractDistributedTransactionManager {

  private final TwoPhaseCommit.Coordinator coordinator;
  @Nullable private final TwoPhaseCommit.Participant participant;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseCommitBackedDistributedTransactionManager(
      DatabaseConfig config,
      TwoPhaseCommit.Coordinator coordinator,
      @Nullable TwoPhaseCommit.Participant participant) {
    super(config);
    this.coordinator = coordinator;
    this.participant = participant;
  }

  @Override
  public DistributedTransaction begin(String txId, Map<String, String> attributes)
      throws TransactionException {
    return beginInternal(txId, false, attributes);
  }

  @Override
  public DistributedTransaction beginReadOnly(String txId, Map<String, String> attributes)
      throws TransactionException {
    return beginInternal(txId, true, attributes);
  }

  private DistributedTransaction beginInternal(
      String txId, boolean readOnly, Map<String, String> attributes) throws TransactionException {
    String canonicalId = coordinator.begin(txId, readOnly, attributes, participant);
    DistributedTransaction transaction = createTransaction(coordinator, participant, canonicalId);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  @Override
  public DistributedTransaction join(String txId) throws TransactionException {
    checkParticipantConfigured();
    coordinator.registerParticipant(txId, participant);
    DistributedTransaction transaction = createTransaction(coordinator, participant, txId);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  /**
   * Creates the {@link DistributedTransaction} instance returned by {@link #begin}, {@link
   * #beginReadOnly}, and {@link #join}. This is a template method: subclasses can override it to
   * customize the transaction instance (for example, to wrap it in a decorator).
   *
   * @param coordinator the coordinator driving the transaction
   * @param participant the in-process participant, or {@code null} if none is configured
   * @param canonicalId the canonical transaction ID
   * @return the transaction instance
   */
  protected DistributedTransaction createTransaction(
      TwoPhaseCommit.Coordinator coordinator,
      @Nullable TwoPhaseCommit.Participant participant,
      String canonicalId) {
    return new TwoPhaseCommitBackedDistributedTransaction(coordinator, participant, canonicalId);
  }

  // The deprecated start(...) overloads carry per-transaction isolation / serializable strategy,
  // which this path does not honor (isolation comes from configuration). Delegate to begin.

  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return begin();
  }

  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return begin(txId);
  }

  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return begin();
  }

  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return begin();
  }

  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId);
  }

  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return begin(txId);
  }

  // ---- TransactionManagerCrudOperable: one-shot CRUD (begin -> op -> commit) ----

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)), true, get.getAttributes());
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(
        t -> t.scan(copyAndSetTargetToIfNot(scan)), true, scan.getAttributes());
  }

  @Override
  public TransactionManagerCrudOperable.Scanner getScanner(Scan scan) throws CrudException {
    checkParticipantConfigured();
    DistributedTransaction transaction = beginOneOperation(true, scan.getAttributes());
    TransactionCrudOperable.Scanner scanner;
    try {
      scanner = transaction.getScanner(copyAndSetTargetToIfNot(scan));
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    }
    return new AbstractTransactionManagerCrudOperableScanner() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public Optional<Result> one() throws CrudException {
        try {
          return scanner.one();
        } catch (CrudException e) {
          closeScannerAndRollback(e);
          throw e;
        }
      }

      @Override
      public List<Result> all() throws CrudException {
        try {
          return scanner.all();
        } catch (CrudException e) {
          closeScannerAndRollback(e);
          throw e;
        }
      }

      @Override
      public void close() throws CrudException, UnknownTransactionStatusException {
        if (!closed.compareAndSet(false, true)) {
          return;
        }
        try {
          scanner.close();
        } catch (CrudException e) {
          rollbackTransaction(transaction);
          throw e;
        }
        try {
          transaction.commit();
        } catch (CommitConflictException e) {
          rollbackTransaction(transaction);
          throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        } catch (UnknownTransactionStatusException e) {
          throw e;
        } catch (TransactionException e) {
          rollbackTransaction(transaction);
          throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
      }

      private void closeScannerAndRollback(CrudException cause) {
        closed.set(true);
        try {
          scanner.close();
        } catch (CrudException ex) {
          cause.addSuppressed(ex);
        }
        rollbackTransaction(transaction);
      }
    };
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        },
        false,
        put.getAttributes());
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        },
        false,
        puts.isEmpty() ? Collections.emptyMap() : puts.get(0).getAttributes());
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        },
        false,
        insert.getAttributes());
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        },
        false,
        upsert.getAttributes());
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        },
        false,
        update.getAttributes());
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        },
        false,
        delete.getAttributes());
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        },
        false,
        deletes.isEmpty() ? Collections.emptyMap() : deletes.get(0).getAttributes());
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        },
        false,
        mutations.isEmpty() ? Collections.emptyMap() : mutations.get(0).getAttributes());
  }

  @Override
  public List<CrudOperable.BatchResult> batch(List<? extends Operation> operations)
      throws CrudException, UnknownTransactionStatusException {
    boolean readOnly = operations.stream().allMatch(o -> o instanceof Selection);
    return executeTransaction(
        t -> t.batch(copyAndSetTargetToIfNot(operations)),
        readOnly,
        operations.isEmpty() ? Collections.emptyMap() : operations.get(0).getAttributes());
  }

  private <R> R executeTransaction(
      ThrowableFunction<DistributedTransaction, R, TransactionException> operation,
      boolean readOnly,
      Map<String, String> attributes)
      throws CrudException, UnknownTransactionStatusException {
    checkParticipantConfigured();
    DistributedTransaction transaction = beginOneOperation(readOnly, attributes);
    try {
      R result = operation.apply(transaction);
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void checkParticipantConfigured() {
    if (participant == null) {
      throw new UnsupportedOperationException(getParticipantNotConfiguredMessage());
    }
  }

  /**
   * Builds the message for the {@link UnsupportedOperationException} thrown when an operation
   * requires a participant but none is configured. This is a template method: subclasses can
   * override it to customize the message.
   *
   * @return the exception message
   */
  protected String getParticipantNotConfiguredMessage() {
    return "This operation is not supported by this two-phase-commit-backed transaction manager"
        + " because no participant is configured";
  }

  private DistributedTransaction beginOneOperation(boolean readOnly, Map<String, String> attributes)
      throws CrudException {
    try {
      return readOnly ? beginReadOnly(attributes) : begin(attributes);
    } catch (TransactionNotFoundException e) {
      // A transient begin failure surfaces as a retriable CRUD conflict.
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void rollbackTransaction(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException ignored) {
      // Best-effort rollback; the original error is the one that matters.
    }
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException(
        "getState is not supported by the two-phase-commit-backed transaction manager");
  }

  @Override
  public boolean finishTransaction(String txId) {
    throw new UnsupportedOperationException(
        "finishTransaction is not supported by the two-phase-commit-backed transaction manager");
  }

  @Override
  public boolean recoverRecord(
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    throw new UnsupportedOperationException(
        "recoverRecord is not supported by the two-phase-commit-backed transaction manager");
  }

  @Override
  public TransactionState rollback(String txId) {
    throw new UnsupportedOperationException(
        "rollback/abort by transaction ID is not supported by the two-phase-commit-backed"
            + " transaction manager");
  }

  @Override
  public void close() {
    coordinator.close();
    if (participant != null) {
      participant.close();
    }
  }
}
