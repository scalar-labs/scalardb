package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@Immutable
public class TwoPhaseCommitTransactionService implements TwoPhaseCommitTransactionManager {
  private final TwoPhaseCommitTransactionManager manager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public TwoPhaseCommitTransactionService(TwoPhaseCommitTransactionManager manager) {
    this.manager = manager;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    manager.with(namespace, tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    manager.withNamespace(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return manager.getNamespace();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    manager.withTable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return manager.getTable();
  }

  @Override
  public TwoPhaseCommitTransaction begin() throws TransactionException {
    return manager.start();
  }

  @Override
  public TwoPhaseCommitTransaction begin(String txId) throws TransactionException {
    return manager.start(txId);
  }

  @Override
  public TwoPhaseCommitTransaction start() throws TransactionException {
    return manager.start();
  }

  @Override
  public TwoPhaseCommitTransaction start(String txId) throws TransactionException {
    return manager.start(txId);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    return manager.join(txId);
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionNotFoundException {
    return manager.resume(txId);
  }

  @Override
  public TransactionState getState(String txId) throws TransactionException {
    return manager.getState(txId);
  }

  @Override
  public TransactionState rollback(String txId) throws TransactionException {
    return manager.rollback(txId);
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return manager.abort(txId);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return manager.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return manager.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    manager.put(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    manager.put(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    manager.insert(insert);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    manager.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    manager.update(update);
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    manager.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    manager.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    manager.mutate(mutations);
  }

  @Override
  public void close() {
    manager.close();
  }
}
