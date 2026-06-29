package com.scalar.db.common;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link TwoPhaseCommit.Participant} decorator that propagates the transaction-scoped attributes
 * supplied at {@link #join} into every CRUD operation issued for that transaction.
 *
 * <p>Begin-attributes reach the participant via {@link
 * TwoPhaseCommit.Coordinator#registerParticipant} → {@code join}; this decorator captures them
 * (keyed by transaction ID) and merges them into each operation before delegating, with an
 * attribute set directly on the operation winning over the transaction-scoped one (see {@link
 * OperationAttributeMerger}). It therefore sits <em>outside</em> any decorator that reads operation
 * attributes (e.g. ABAC). The captured attributes are dropped on the transaction's terminal step
 * ({@code commitRecords} / {@code rollbackRecords} / {@code releaseContext}).
 *
 * <p>The record-level steps ({@code prepareRecords} / {@code validateRecords}) and non-CRUD methods
 * are forwarded unchanged.
 *
 * <p>The captured attributes are released only on a terminal step driven through this decorator. A
 * transaction abandoned without one (e.g. a crashed client) leaves its entry until the JVM exits.
 * This is reaped only when {@link ActiveTransactionManagedTwoPhaseCommitParticipant} wraps this
 * decorator (its idle-expiry calls {@code releaseContext}, which clears the entry); the leak is in
 * lockstep with the wrapped participant's own per-transaction context, which active transaction
 * management exists to reap. Enable active transaction management whenever this decorator is used.
 */
@ThreadSafe
public class AttributePropagatingTwoPhaseCommitParticipant
    extends DecoratedTwoPhaseCommitParticipant {

  private final ConcurrentMap<String, Map<String, String>> transactionAttributes =
      new ConcurrentHashMap<>();

  public AttributePropagatingTwoPhaseCommitParticipant(TwoPhaseCommit.Participant participant) {
    super(participant);
  }

  @Override
  public void join(String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    super.join(transactionId, readOnly, attributes);
    // Capture only after a successful join so a failed join leaves no stale entry. Skip empty
    // attributes so there is nothing to merge later.
    if (!attributes.isEmpty()) {
      transactionAttributes.put(transactionId, ImmutableMap.copyOf(attributes));
    }
  }

  @Override
  public Optional<Result> get(String transactionId, Get get)
      throws CrudException, TransactionNotFoundException {
    return super.get(transactionId, merge(transactionId, get));
  }

  @Override
  public List<Result> scan(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    return super.scan(transactionId, merge(transactionId, scan));
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    return super.getScanner(transactionId, merge(transactionId, scan));
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(String transactionId, Put put)
      throws CrudException, TransactionNotFoundException {
    super.put(transactionId, merge(transactionId, put));
  }

  @Override
  public void insert(String transactionId, Insert insert)
      throws CrudException, TransactionNotFoundException {
    super.insert(transactionId, merge(transactionId, insert));
  }

  @Override
  public void upsert(String transactionId, Upsert upsert)
      throws CrudException, TransactionNotFoundException {
    super.upsert(transactionId, merge(transactionId, upsert));
  }

  @Override
  public void update(String transactionId, Update update)
      throws CrudException, TransactionNotFoundException {
    super.update(transactionId, merge(transactionId, update));
  }

  @Override
  public void delete(String transactionId, Delete delete)
      throws CrudException, TransactionNotFoundException {
    super.delete(transactionId, merge(transactionId, delete));
  }

  @Override
  public void mutate(String transactionId, List<? extends Mutation> mutations)
      throws CrudException, TransactionNotFoundException {
    super.mutate(
        transactionId, OperationAttributeMerger.mergeEach(mutations, attributesFor(transactionId)));
  }

  @Override
  public List<CrudOperable.BatchResult> batch(
      String transactionId, List<? extends Operation> operations)
      throws CrudException, TransactionNotFoundException {
    return super.batch(
        transactionId,
        OperationAttributeMerger.mergeEach(operations, attributesFor(transactionId)));
  }

  @Override
  public void commitRecords(String transactionId, long committedAt)
      throws CommitException, TransactionNotFoundException {
    try {
      super.commitRecords(transactionId, committedAt);
    } finally {
      transactionAttributes.remove(transactionId);
    }
  }

  @Override
  public void rollbackRecords(String transactionId) throws RollbackException {
    try {
      super.rollbackRecords(transactionId);
    } finally {
      transactionAttributes.remove(transactionId);
    }
  }

  @Override
  public void releaseContext(String transactionId) {
    try {
      super.releaseContext(transactionId);
    } finally {
      transactionAttributes.remove(transactionId);
    }
  }

  private <T extends Operation> T merge(String transactionId, T operation) {
    return OperationAttributeMerger.merge(operation, attributesFor(transactionId));
  }

  private Map<String, String> attributesFor(String transactionId) {
    return transactionAttributes.getOrDefault(transactionId, Collections.emptyMap());
  }
}
