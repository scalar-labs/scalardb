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
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A decorator that registers every transaction it begins or starts, so that a server continuing a
 * transaction across requests can get it back by its ID with {@link #resume(String)}. A registered
 * transaction is removed when a commit, a rollback, or an abort on it completes, whether it
 * succeeds or fails, since each of them ends the transaction. A registered transaction is also
 * disposed of when it expires or is evicted.
 *
 * <p>It also tracks every scanner that it opens outside a transaction. Such a scanner runs in a
 * transaction of its own, which only its close ends, so a scanner is closed when it expires or is
 * evicted.
 *
 * <p>{@link #resume(String)} returns a transaction exactly as this manager registered it, so wrap
 * this manager outside every decorator whose behavior a resumed transaction must carry. The
 * exception is a decorator that can fail a commit, a rollback, or an abort without passing it on:
 * place it outside this manager, and have it wrap the transactions that {@code resume} returns
 * itself. Otherwise, this manager would remove a transaction that the rejected call never reached,
 * and nothing would end that transaction.
 */
@ThreadSafe
public class ActiveTransactionManagedDistributedTransactionManager
    extends DecoratedDistributedTransactionManager
    implements ResumableDistributedTransactionManager {

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedDistributedTransactionManager.class);

  private final ActiveTransactionRegistry<DistributedTransaction> registry;
  private final ActiveExpiringMap<String, ActiveScanner> activeScanners;

  public ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager,
      long expirationTimeMillis,
      int maxActiveTransactions) {
    super(transactionManager);
    registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis, maxActiveTransactions, DistributedTransaction::rollback);
    activeScanners = newActiveScanners(expirationTimeMillis, maxActiveTransactions);
  }

  public ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager,
      long expirationTimeMillis,
      int maxActiveTransactions,
      ActiveTransactionRegistry.DisposalHandler<DistributedTransaction> disposalHandler) {
    super(transactionManager);
    registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis, maxActiveTransactions, disposalHandler);
    activeScanners = newActiveScanners(expirationTimeMillis, maxActiveTransactions);
  }

  @VisibleForTesting
  ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager,
      ActiveTransactionRegistry<DistributedTransaction> registry) {
    this(transactionManager, registry, newActiveScanners(-1, -1));
  }

  @VisibleForTesting
  ActiveTransactionManagedDistributedTransactionManager(
      DistributedTransactionManager transactionManager,
      ActiveTransactionRegistry<DistributedTransaction> registry,
      ActiveExpiringMap<String, ActiveScanner> activeScanners) {
    super(transactionManager);
    this.registry = registry;
    this.activeScanners = activeScanners;
  }

  // Scanners opened outside a transaction have a cap of their own, the same value as the cap on the
  // transactions, so that a flood of abandoned scanners never evicts a transaction, nor the other
  // way around. A scanner opened in a transaction is part of that transaction and is not tracked
  // here.
  private static ActiveExpiringMap<String, ActiveScanner> newActiveScanners(
      long expirationTimeMillis, int maxActiveScanners) {
    return new ActiveExpiringMap<>(
        expirationTimeMillis,
        maxActiveScanners,
        (id, scanner) -> {
          logger.warn("The scanner is expired");
          scanner.dispose();
        },
        (id, scanner) -> {
          logger.warn(
              "The scanner is evicted because the number of scanners opened outside a transaction"
                  + " exceeded the maximum ({}), which is the same as the maximum number of active"
                  + " transactions",
              maxActiveScanners);
          scanner.dispose();
        });
  }

  @Override
  protected DistributedTransaction decorateTransactionOnBeginOrStart(
      DistributedTransaction transaction) throws TransactionException {
    return new ActiveTransaction(transaction);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return new ActiveScanner(super.getScanner(scan));
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
    public synchronized void commit() throws CommitException, UnknownTransactionStatusException {
      // A commit ends the transaction whatever its outcome
      try {
        super.commit();
      } finally {
        registry.remove(getId());
      }
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

  /**
   * A scanner opened outside a transaction. Its methods are synchronized because the expiration or
   * eviction handler may close it in a different thread while another method is being executed.
   */
  @VisibleForTesting
  class ActiveScanner extends AbstractTransactionManagerCrudOperableScanner {

    private final String id = UUID.randomUUID().toString();
    private final TransactionManagerCrudOperable.Scanner scanner;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveScanner(TransactionManagerCrudOperable.Scanner scanner) {
      this.scanner = scanner;
      activeScanners.put(id, this);
    }

    @Override
    public synchronized Optional<Result> one() throws CrudException {
      activeScanners.updateExpirationTime(id);
      return scanner.one();
    }

    @Override
    public synchronized List<Result> all() throws CrudException {
      activeScanners.updateExpirationTime(id);
      return scanner.all();
    }

    @Override
    public synchronized void close() throws CrudException, UnknownTransactionStatusException {
      // Closing the scanner ends its transaction whatever the outcome
      try {
        scanner.close();
      } finally {
        activeScanners.remove(id);
      }
    }

    // Catches every exception, so that a failed close does not escape the expiration or eviction
    // handler
    synchronized void dispose() {
      try {
        scanner.close();
      } catch (Exception e) {
        logger.warn("Failed to close the expired or evicted scanner", e);
      }
    }
  }
}
