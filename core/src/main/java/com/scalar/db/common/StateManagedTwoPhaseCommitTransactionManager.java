package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class StateManagedTwoPhaseCommitTransactionManager
    extends DecoratedTwoPhaseCommitTransactionManager {

  public StateManagedTwoPhaseCommitTransactionManager(
      TwoPhaseCommitTransactionManager transactionManager) {
    super(transactionManager);
  }

  @Override
  protected TwoPhaseCommitTransaction decorateTransactionOnBeginOrStart(
      TwoPhaseCommitTransaction transaction) {
    return new StateManagedTransaction(transaction);
  }

  /**
   * This class is to unify the call sequence of the transaction object. It doesn't care about the
   * potential inconsistency between the status field on JVM memory and the underlying persistent
   * layer.
   */
  @VisibleForTesting
  static class StateManagedTransaction extends DecoratedTwoPhaseCommitTransaction {

    private enum Status {
      ACTIVE,
      PREPARED,
      PREPARE_FAILED,
      VALIDATED,
      VALIDATION_FAILED,
      COMMITTED,
      COMMIT_FAILED,
      ROLLED_BACK
    }

    private Status status;

    @VisibleForTesting
    StateManagedTransaction(TwoPhaseCommitTransaction transaction) {
      super(transaction);
      status = Status.ACTIVE;
    }

    @Override
    public Optional<Result> get(Get get) throws CrudException {
      checkIfActive();
      return super.get(get);
    }

    @Override
    public List<Result> scan(Scan scan) throws CrudException {
      checkIfActive();
      return super.scan(scan);
    }

    @Override
    public Scanner getScanner(Scan scan) throws CrudException {
      checkIfActive();
      return super.getScanner(scan);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void put(Put put) throws CrudException {
      checkIfActive();
      super.put(put);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void put(List<Put> puts) throws CrudException {
      checkIfActive();
      super.put(puts);
    }

    @Override
    public void delete(Delete delete) throws CrudException {
      checkIfActive();
      super.delete(delete);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void delete(List<Delete> deletes) throws CrudException {
      checkIfActive();
      super.delete(deletes);
    }

    @Override
    public void insert(Insert insert) throws CrudException {
      checkIfActive();
      super.insert(insert);
    }

    @Override
    public void upsert(Upsert upsert) throws CrudException {
      checkIfActive();
      super.upsert(upsert);
    }

    @Override
    public void update(Update update) throws CrudException {
      checkIfActive();
      super.update(update);
    }

    @Override
    public void mutate(List<? extends Mutation> mutations) throws CrudException {
      checkIfActive();
      super.mutate(mutations);
    }

    @Override
    public void prepare() throws PreparationException {
      checkIfActive();
      try {
        super.prepare();
        status = Status.PREPARED;
      } catch (Exception e) {
        status = Status.PREPARE_FAILED;
        throw e;
      }
    }

    @Override
    public void validate() throws ValidationException {
      if (status != Status.PREPARED) {
        throw new IllegalStateException(CoreError.TRANSACTION_NOT_PREPARED.buildMessage(status));
      }

      try {
        super.validate();
        status = Status.VALIDATED;
      } catch (Exception e) {
        status = Status.VALIDATION_FAILED;
        throw e;
      }
    }

    @Override
    public void commit() throws CommitException, UnknownTransactionStatusException {
      if (status != Status.PREPARED && status != Status.VALIDATED) {
        throw new IllegalStateException(
            CoreError.TRANSACTION_NOT_PREPARED_OR_VALIDATED.buildMessage(status));
      }

      try {
        super.commit();
        status = Status.COMMITTED;
      } catch (Exception e) {
        status = Status.COMMIT_FAILED;
        throw e;
      }
    }

    @Override
    public void rollback() throws RollbackException {
      if (status == Status.ROLLED_BACK) {
        return;
      }
      if (status == Status.COMMITTED) {
        throw new IllegalStateException(
            CoreError.TRANSACTION_ALREADY_COMMITTED.buildMessage(status));
      }
      try {
        super.rollback();
      } finally {
        status = Status.ROLLED_BACK;
      }
    }

    @Override
    public void abort() throws AbortException {
      if (status == Status.ROLLED_BACK) {
        return;
      }
      if (status == Status.COMMITTED) {
        throw new IllegalStateException(
            CoreError.TRANSACTION_ALREADY_COMMITTED.buildMessage(status));
      }
      try {
        super.abort();
      } finally {
        status = Status.ROLLED_BACK;
      }
    }

    private void checkIfActive() {
      if (status != Status.ACTIVE) {
        throw new IllegalStateException(CoreError.TRANSACTION_NOT_ACTIVE.buildMessage(status));
      }
    }
  }
}
