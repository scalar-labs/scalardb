package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;

public abstract class DecoratedDistributedTransaction implements DistributedTransaction {

  private final DistributedTransaction decoratedTransaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
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

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    decoratedTransaction.put(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    decoratedTransaction.put(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    decoratedTransaction.delete(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    decoratedTransaction.delete(deletes);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    decoratedTransaction.insert(insert);
  }

  @Override
  public void update(Update update) throws CrudException {
    decoratedTransaction.update(update);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    decoratedTransaction.upsert(upsert);
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

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public DistributedTransaction getOriginalTransaction() {
    if (decoratedTransaction instanceof DecoratedDistributedTransaction) {
      return ((DecoratedDistributedTransaction) decoratedTransaction).getOriginalTransaction();
    }
    return decoratedTransaction;
  }
}
