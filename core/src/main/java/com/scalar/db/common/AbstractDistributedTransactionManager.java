package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;

public abstract class AbstractDistributedTransactionManager
    implements DistributedTransactionManager, DistributedTransactionDecoratorAddable {

  private Optional<String> namespace;
  private Optional<String> tableName;

  private final List<DistributedTransactionDecorator> transactionDecorators =
      new CopyOnWriteArrayList<>();

  public AbstractDistributedTransactionManager(DatabaseConfig databaseConfig) {
    namespace = databaseConfig.getDefaultNamespaceName();
    tableName = Optional.empty();

    addTransactionDecorator(StateManagedTransaction::new);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  protected DistributedTransaction decorate(DistributedTransaction transaction)
      throws TransactionException {
    DistributedTransaction decorated = transaction;
    for (DistributedTransactionDecorator transactionDecorator : transactionDecorators) {
      decorated = transactionDecorator.decorate(decorated);
    }
    return decorated;
  }

  @Override
  public void addTransactionDecorator(DistributedTransactionDecorator transactionDecorator) {
    transactionDecorators.add(transactionDecorator);
  }

  /**
   * This class is to unify the call sequence of the transaction object. It doesn't care about the
   * potential inconsistency between the status field on JVM memory and the underlying persistent
   * layer.
   */
  @VisibleForTesting
  static class StateManagedTransaction extends DecoratedDistributedTransaction {

    private enum Status {
      ACTIVE,
      COMMITTED,
      COMMIT_FAILED,
      ROLLED_BACK
    }

    private Status status;

    @VisibleForTesting
    StateManagedTransaction(DistributedTransaction transaction) {
      super(transaction);
      status = Status.ACTIVE;
    }

    @Override
    public Optional<Result> get(Get get) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      return super.get(get);
    }

    @Override
    public List<Result> scan(Scan scan) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      return super.scan(scan);
    }

    @Override
    public void put(Put put) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      super.put(put);
    }

    @Override
    public void put(List<Put> puts) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      super.put(puts);
    }

    @Override
    public void delete(Delete delete) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      super.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      super.delete(deletes);
    }

    @Override
    public void mutate(List<? extends Mutation> mutations) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      super.mutate(mutations);
    }

    @Override
    public void commit() throws CommitException, UnknownTransactionStatusException {
      checkStatus("The transaction is not active", Status.ACTIVE);
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
      if (status == Status.COMMITTED || status == Status.ROLLED_BACK) {
        throw new IllegalStateException(
            "The transaction has already been committed or rolled back");
      }
      try {
        super.rollback();
      } finally {
        status = Status.ROLLED_BACK;
      }
    }

    @Override
    public void abort() throws AbortException {
      if (status == Status.COMMITTED || status == Status.ROLLED_BACK) {
        throw new IllegalStateException("The transaction has already been committed or aborted");
      }
      try {
        super.abort();
      } finally {
        status = Status.ROLLED_BACK;
      }
    }

    private void checkStatus(@Nullable String message, Status... expectedStatus) {
      boolean expected = Arrays.stream(expectedStatus).anyMatch(s -> status == s);
      if (!expected) {
        throw new IllegalStateException(message);
      }
    }
  }
}
