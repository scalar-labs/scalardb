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
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitTransactionManager
    extends DecoratedTwoPhaseCommitTransactionManager {

  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedTwoPhaseCommitTransactionManager.class);

  private final ActiveExpiringMap<String, ActiveTransaction> activeTransactions;

  private final AtomicReference<BiConsumer<String, TwoPhaseCommitTransaction>>
      transactionExpirationHandler =
          new AtomicReference<>(
              (id, t) -> {
                try {
                  t.rollback();
                } catch (Exception e) {
                  logger.warn("Rollback failed. Transaction ID: {}", id, e);
                }
              });

  public ActiveTransactionManagedTwoPhaseCommitTransactionManager(
      TwoPhaseCommitTransactionManager transactionManager,
      long activeTransactionManagementExpirationTimeMillis) {
    super(transactionManager);
    activeTransactions =
        new ActiveExpiringMap<>(
            activeTransactionManagementExpirationTimeMillis,
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            (id, t) -> {
              logger.warn("The transaction is expired. Transaction ID: {}", id);
              transactionExpirationHandler.get().accept(id, t);
            });
  }

  @Override
  public void setTransactionExpirationHandler(
      BiConsumer<String, TwoPhaseCommitTransaction> handler) {
    transactionExpirationHandler.set(handler);
  }

  private void add(ActiveTransaction transaction) throws TransactionException {
    if (activeTransactions.putIfAbsent(transaction.getId(), transaction).isPresent()) {
      transaction.rollback();
      throw new TransactionException(
          CoreError.TRANSACTION_ALREADY_EXISTS.buildMessage(), transaction.getId());
    }
  }

  private void remove(String transactionId) {
    activeTransactions.remove(transactionId);
  }

  @Override
  protected TwoPhaseCommitTransaction decorateTransactionOnBeginOrStart(
      TwoPhaseCommitTransaction transaction) throws TransactionException {
    return new ActiveTransaction(transaction);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    if (activeTransactions.containsKey(txId)) {
      return resume(txId);
    }
    return new ActiveTransaction(super.join(txId));
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionNotFoundException {
    return activeTransactions
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
      add(this);
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
      Scanner scanner = super.getScanner(scan);
      return new Scanner() {
        @Override
        public Optional<Result> one() throws CrudException {
          synchronized (ActiveTransaction.this) {
            return scanner.one();
          }
        }

        @Override
        public List<Result> all() throws CrudException {
          synchronized (ActiveTransaction.this) {
            return scanner.all();
          }
        }

        @Override
        public void close() throws CrudException {
          synchronized (ActiveTransaction.this) {
            scanner.close();
          }
        }

        @Nonnull
        @Override
        public Iterator<Result> iterator() {
          synchronized (ActiveTransaction.this) {
            return scanner.iterator();
          }
        }
      };
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
    public synchronized void prepare() throws PreparationException {
      super.prepare();
    }

    @Override
    public synchronized void validate() throws ValidationException {
      super.validate();
    }

    @Override
    public synchronized void commit() throws CommitException, UnknownTransactionStatusException {
      super.commit();
      remove(getId());
    }

    @Override
    public synchronized void rollback() throws RollbackException {
      try {
        super.rollback();
      } finally {
        remove(getId());
      }
    }

    @Override
    public synchronized void abort() throws AbortException {
      try {
        super.abort();
      } finally {
        remove(getId());
      }
    }
  }
}
