package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@ThreadSafe
public class TransactionService implements DistributedTransactionManager {
  private final DistributedTransactionManager manager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public TransactionService(DistributedTransactionManager manager) {
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
  public DistributedTransaction begin() throws TransactionException {
    return manager.begin();
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    return manager.begin(txId);
  }

  @Override
  public DistributedTransaction start() throws TransactionException {
    return manager.start();
  }

  @Override
  public DistributedTransaction start(String txId) throws TransactionException {
    return manager.start(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return manager.start(isolation);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return manager.start(txId, isolation);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return manager.start(isolation, strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return manager.start(strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return manager.start(txId, strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return manager.start(txId, isolation, strategy);
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
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
  public Scanner getScanner(Scan scan) throws CrudException {
    return manager.getScanner(scan);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    manager.put(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
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

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
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
