package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ActiveTransactionManagedDistributedTransactionManager
    extends DecoratedDistributedTransactionManager {

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedDistributedTransactionManager.class);

  private final ActiveTransactionRegistry<DistributedTransaction> registry;

  public ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager, long expirationTimeMillis) {
    super(transactionManager);
    registry =
        new ActiveTransactionRegistry<>(expirationTimeMillis, DistributedTransaction::rollback);
  }

  public ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager,
      long expirationTimeMillis,
      ActiveTransactionRegistry.TransactionRollback<DistributedTransaction> rollbackFunction) {
    super(transactionManager);
    registry = new ActiveTransactionRegistry<>(expirationTimeMillis, rollbackFunction);
  }

  @Override
  protected DistributedTransaction decorateTransactionOnBeginOrStart(
      DistributedTransaction transaction) throws TransactionException {
    return new ActiveTransaction(transaction);
  }

  @Override
  public DistributedTransaction join(String txId) throws TransactionNotFoundException {
    return resume(txId);
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
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
  class ActiveTransaction extends DecoratedDistributedTransaction {

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveTransaction(DistributedTransaction transaction) throws TransactionException {
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
      return super.get(get);
    }

    @Override
    public synchronized List<Result> scan(Scan scan) throws CrudException {
      return super.scan(scan);
    }

    @Override
    public synchronized Scanner getScanner(Scan scan) throws CrudException {
      return new SynchronizedScanner(this, super.getScanner(scan));
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public synchronized void put(Put put) throws CrudException {
      super.put(put);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public synchronized void put(List<Put> puts) throws CrudException {
      super.put(puts);
    }

    @Override
    public synchronized void delete(Delete delete) throws CrudException {
      super.delete(delete);
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public synchronized void delete(List<Delete> deletes) throws CrudException {
      super.delete(deletes);
    }

    @Override
    public synchronized void insert(Insert insert) throws CrudException {
      super.insert(insert);
    }

    @Override
    public synchronized void upsert(Upsert upsert) throws CrudException {
      super.upsert(upsert);
    }

    @Override
    public synchronized void update(Update update) throws CrudException {
      super.update(update);
    }

    @Override
    public synchronized void mutate(List<? extends Mutation> mutations) throws CrudException {
      super.mutate(mutations);
    }

    @Override
    public synchronized List<BatchResult> batch(List<? extends Operation> operations)
        throws CrudException {
      return super.batch(operations);
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
