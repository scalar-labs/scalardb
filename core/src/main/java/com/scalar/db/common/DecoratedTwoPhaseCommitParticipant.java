package com.scalar.db.common;

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
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link TwoPhaseCommit.Participant} that forwards every method to a wrapped participant.
 *
 * <p>Base class for participant decorators: subclasses override only the methods they need and
 * inherit plain delegation for the rest. It is the id-based {@link TwoPhaseCommit.Participant}
 * counterpart of the manager-API {@link DecoratedDistributedTransactionManager}.
 */
public abstract class DecoratedTwoPhaseCommitParticipant implements TwoPhaseCommit.Participant {

  private final TwoPhaseCommit.Participant participant;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedTwoPhaseCommitParticipant(TwoPhaseCommit.Participant participant) {
    this.participant = participant;
  }

  @Override
  public String getId() {
    return participant.getId();
  }

  @Override
  public void join(String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    participant.join(transactionId, readOnly, attributes);
  }

  @Override
  public Optional<Result> get(String transactionId, Get get)
      throws CrudException, TransactionNotFoundException {
    return participant.get(transactionId, get);
  }

  @Override
  public List<Result> scan(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    return participant.scan(transactionId, scan);
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    return participant.getScanner(transactionId, scan);
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(String transactionId, Put put)
      throws CrudException, TransactionNotFoundException {
    participant.put(transactionId, put);
  }

  @Override
  public void insert(String transactionId, Insert insert)
      throws CrudException, TransactionNotFoundException {
    participant.insert(transactionId, insert);
  }

  @Override
  public void upsert(String transactionId, Upsert upsert)
      throws CrudException, TransactionNotFoundException {
    participant.upsert(transactionId, upsert);
  }

  @Override
  public void update(String transactionId, Update update)
      throws CrudException, TransactionNotFoundException {
    participant.update(transactionId, update);
  }

  @Override
  public void delete(String transactionId, Delete delete)
      throws CrudException, TransactionNotFoundException {
    participant.delete(transactionId, delete);
  }

  @Override
  public void mutate(String transactionId, List<? extends Mutation> mutations)
      throws CrudException, TransactionNotFoundException {
    participant.mutate(transactionId, mutations);
  }

  @Override
  public List<CrudOperable.BatchResult> batch(
      String transactionId, List<? extends Operation> operations)
      throws CrudException, TransactionNotFoundException {
    return participant.batch(transactionId, operations);
  }

  @Override
  public TwoPhaseCommit.PreparationResult prepareRecords(
      String transactionId, long preparedAt, TwoPhaseCommit.WriteSetDetailLevel detailLevel)
      throws PreparationException, TransactionNotFoundException {
    return participant.prepareRecords(transactionId, preparedAt, detailLevel);
  }

  @Override
  public void validateRecords(String transactionId)
      throws ValidationException, TransactionNotFoundException {
    participant.validateRecords(transactionId);
  }

  @Override
  public void commitRecords(String transactionId, long committedAt)
      throws CommitException, TransactionNotFoundException {
    participant.commitRecords(transactionId, committedAt);
  }

  @Override
  public void rollbackRecords(String transactionId) throws RollbackException {
    participant.rollbackRecords(transactionId);
  }

  @Override
  public void releaseContext(String transactionId) {
    participant.releaseContext(transactionId);
  }

  @Override
  public void close() {
    participant.close();
  }
}
