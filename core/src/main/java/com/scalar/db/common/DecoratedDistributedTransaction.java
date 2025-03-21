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

  private final DistributedTransaction transaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DecoratedDistributedTransaction(DistributedTransaction transaction) {
    this.transaction = transaction;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    transaction.with(namespace, tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    transaction.withNamespace(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return transaction.getNamespace();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    transaction.withTable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return transaction.getTable();
  }

  @Override
  public String getId() {
    return transaction.getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return transaction.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return transaction.scan(scan);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return transaction.getScanner(scan);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    transaction.put(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    transaction.put(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    transaction.delete(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    transaction.delete(deletes);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    transaction.insert(insert);
  }

  @Override
  public void update(Update update) throws CrudException {
    transaction.update(update);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    transaction.upsert(upsert);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    transaction.mutate(mutations);
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    transaction.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    transaction.rollback();
  }

  @Override
  public void abort() throws AbortException {
    transaction.abort();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public DistributedTransaction getOriginalTransaction() {
    if (transaction instanceof DecoratedDistributedTransaction) {
      return ((DecoratedDistributedTransaction) transaction).getOriginalTransaction();
    }
    return transaction;
  }
}
