package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.List;
import java.util.Optional;

public abstract class DecoratedDistributedTransaction implements DistributedTransaction {

  private final DistributedTransaction decoratedTransaction;

  public DecoratedDistributedTransaction(DistributedTransaction decoratedTransaction) {
    this.decoratedTransaction = decoratedTransaction;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    decoratedTransaction.with(namespace, tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    decoratedTransaction.withNamespace(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return decoratedTransaction.getNamespace();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    decoratedTransaction.withTable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return decoratedTransaction.getTable();
  }

  @Override
  public String getId() {
    return decoratedTransaction.getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return decoratedTransaction.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return decoratedTransaction.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException {
    decoratedTransaction.put(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    decoratedTransaction.put(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    decoratedTransaction.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    decoratedTransaction.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    decoratedTransaction.mutate(mutations);
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    decoratedTransaction.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    decoratedTransaction.rollback();
  }

  @Override
  public void abort() throws AbortException {
    decoratedTransaction.abort();
  }

  public DistributedTransaction getOriginalTransaction() {
    if (decoratedTransaction instanceof DecoratedDistributedTransaction) {
      return ((DecoratedDistributedTransaction) decoratedTransaction).getOriginalTransaction();
    }
    return decoratedTransaction;
  }
}
