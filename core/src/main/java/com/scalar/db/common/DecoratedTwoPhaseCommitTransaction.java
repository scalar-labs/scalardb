package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;

public abstract class DecoratedTwoPhaseCommitTransaction implements TwoPhaseCommitTransaction {

  private final TwoPhaseCommitTransaction transaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DecoratedTwoPhaseCommitTransaction(TwoPhaseCommitTransaction transaction) {
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
  public void upsert(Upsert upsert) throws CrudException {
    transaction.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException {
    transaction.update(update);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    transaction.mutate(mutations);
  }

  @Override
  public void prepare() throws PreparationException {
    transaction.prepare();
  }

  @Override
  public void validate() throws ValidationException {
    transaction.validate();
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
  public TwoPhaseCommitTransaction getOriginalTransaction() {
    if (transaction instanceof DecoratedTwoPhaseCommitTransaction) {
      return ((DecoratedTwoPhaseCommitTransaction) transaction).getOriginalTransaction();
    }
    return transaction;
  }
}
