package com.scalar.db.common;

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
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.ThrowableFunction;
import java.util.List;
import java.util.Optional;

public abstract class AbstractTwoPhaseCommitTransactionManager
    implements TwoPhaseCommitTransactionManager {

  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractTwoPhaseCommitTransactionManager(DatabaseConfig config) {
    namespace = config.getDefaultNamespaceName();
    tableName = Optional.empty();
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

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.scan(copyAndSetTargetToIfNot(scan)));
  }

  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        });
  }

  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        });
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        });
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        });
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        });
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        });
  }

  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        });
  }

  private <R> R executeTransaction(
      ThrowableFunction<TwoPhaseCommitTransaction, R, TransactionException> throwableFunction)
      throws CrudException, UnknownTransactionStatusException {
    TwoPhaseCommitTransaction transaction;
    try {
      transaction = begin();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    try {
      R result = throwableFunction.apply(transaction);
      transaction.prepare();
      transaction.validate();
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (PreparationConflictException
        | ValidationConflictException
        | CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void rollbackTransaction(TwoPhaseCommitTransaction transaction) throws CrudException {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  protected <T extends Mutation> List<T> copyAndSetTargetToIfNot(List<T> mutations) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(mutations, namespace, tableName);
  }

  protected Get copyAndSetTargetToIfNot(Get get) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(get, namespace, tableName);
  }

  protected Scan copyAndSetTargetToIfNot(Scan scan) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(scan, namespace, tableName);
  }

  protected Put copyAndSetTargetToIfNot(Put put) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(put, namespace, tableName);
  }

  protected Delete copyAndSetTargetToIfNot(Delete delete) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(delete, namespace, tableName);
  }

  protected Insert copyAndSetTargetToIfNot(Insert insert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(insert, namespace, tableName);
  }

  protected Upsert copyAndSetTargetToIfNot(Upsert upsert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(upsert, namespace, tableName);
  }

  protected Update copyAndSetTargetToIfNot(Update update) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(update, namespace, tableName);
  }
}
