package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
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
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitTransactionManager
    extends DecoratedTwoPhaseCommitTransactionManager {

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedTwoPhaseCommitTransactionManager.class);

  private final ActiveTransactionRegistry<TwoPhaseCommitTransaction> registry;

  public ActiveTransactionManagedTwoPhaseCommitTransactionManager(
      TwoPhaseCommitTransactionManager transactionManager,
      long expirationTimeMillis,
      int maxActiveTransactions) {
    super(transactionManager);
    registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis, maxActiveTransactions, TwoPhaseCommitTransaction::rollback);
  }

  public ActiveTransactionManagedTwoPhaseCommitTransactionManager(
      TwoPhaseCommitTransactionManager transactionManager,
      long expirationTimeMillis,
      int maxActiveTransactions,
      ActiveTransactionRegistry.DisposalHandler<TwoPhaseCommitTransaction> disposalHandler) {
    super(transactionManager);
    registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis, maxActiveTransactions, disposalHandler);
  }

  @VisibleForTesting
  ActiveTransactionManagedTwoPhaseCommitTransactionManager(
      TwoPhaseCommitTransactionManager transactionManager,
      ActiveTransactionRegistry<TwoPhaseCommitTransaction> registry) {
    super(transactionManager);
    this.registry = registry;
  }

  @Override
  protected TwoPhaseCommitTransaction decorateTransactionOnBeginOrStart(
      TwoPhaseCommitTransaction transaction) throws TransactionException {
    return new ActiveTransaction(transaction);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    Optional<TwoPhaseCommitTransaction> transaction = registry.get(txId);
    if (transaction.isPresent()) {
      return transaction.get();
    }
    return new ActiveTransaction(super.join(txId));
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionNotFoundException {
    return registry
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionNotFoundException(
                    CoreError.TRANSACTION_NOT_FOUND.buildMessage(), txId));
  }

  /**
   * The methods of this class are synchronized to be thread-safe because the rollback() method may
   * be called from the expiration handler in a different thread while other methods are being
   * executed.
   */
  @VisibleForTesting
  class ActiveTransaction extends DecoratedTwoPhaseCommitTransaction {

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveTransaction(TwoPhaseCommitTransaction transaction) throws TransactionException {
      super(transaction);
      if (!registry.add(getId(), this)) {
        try {
          transaction.rollback();
        } catch (RollbackException e) {
          logger.warn(
              "Rollback failed during duplicate transaction handling. Transaction ID: {}",
              getId(),
              e);
        }
        throw new TransactionException(
            CoreError.TRANSACTION_ALREADY_EXISTS.buildMessage(), getId());
      }
    }

    @Override
    public synchronized Optional<Result> get(Get get) throws CrudException {
      registry.touch(getId());
      return super.get(get);
    }

    @Override
    public synchronized List<Result> scan(Scan scan) throws CrudException {
      registry.touch(getId());
      return super.scan(scan);
    }

    @Override
    public synchronized Scanner getScanner(Scan scan) throws CrudException {
      registry.touch(getId());
      // Refresh the idle timer on each read so a slowly-consumed scan is not reaped mid-iteration,
      // then synchronize the read against a concurrent reaper rollback (SynchronizedScanner).
      return new ActiveTransactionRefreshingScanner(
          registry, getId(), new SynchronizedScanner(this, super.getScanner(scan)));
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
    @Deprecated
    @Override
    public synchronized void put(Put put) throws CrudException {
      registry.touch(getId());
      super.put(put);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
    @Deprecated
    @Override
    public synchronized void put(List<Put> puts) throws CrudException {
      registry.touch(getId());
      super.put(puts);
    }

    @Override
    public synchronized void delete(Delete delete) throws CrudException {
      registry.touch(getId());
      super.delete(delete);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
    @Deprecated
    @Override
    public synchronized void delete(List<Delete> deletes) throws CrudException {
      registry.touch(getId());
      super.delete(deletes);
    }

    @Override
    public synchronized void insert(Insert insert) throws CrudException {
      registry.touch(getId());
      super.insert(insert);
    }

    @Override
    public synchronized void upsert(Upsert upsert) throws CrudException {
      registry.touch(getId());
      super.upsert(upsert);
    }

    @Override
    public synchronized void update(Update update) throws CrudException {
      registry.touch(getId());
      super.update(update);
    }

    @Override
    public synchronized void mutate(List<? extends Mutation> mutations) throws CrudException {
      registry.touch(getId());
      super.mutate(mutations);
    }

    @Override
    public synchronized List<BatchResult> batch(List<? extends Operation> operations)
        throws CrudException {
      registry.touch(getId());
      return super.batch(operations);
    }

    @Override
    public synchronized void prepare() throws PreparationException {
      registry.touch(getId());
      super.prepare();
    }

    @Override
    public synchronized void validate() throws ValidationException {
      registry.touch(getId());
      super.validate();
    }

    @Override
    public synchronized void commit() throws CommitException, UnknownTransactionStatusException {
      super.commit();
      registry.remove(getId());
    }

    @Override
    public synchronized void rollback() throws RollbackException {
      try {
        super.rollback();
      } finally {
        registry.remove(getId());
      }
    }

    @Override
    public synchronized void abort() throws AbortException {
      try {
        super.abort();
      } finally {
        registry.remove(getId());
      }
    }
  }
}
