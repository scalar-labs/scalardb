package com.scalar.db.common;

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
import java.util.List;
import java.util.Optional;

public abstract class DecoratedDistributedTransactionManager
    implements DistributedTransactionManager {

  private final DistributedTransactionManager decoratedTransactionManager;

  public DecoratedDistributedTransactionManager(
      DistributedTransactionManager decoratedTransactionManager) {
    this.decoratedTransactionManager = decoratedTransactionManager;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    decoratedTransactionManager.with(namespace, tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    decoratedTransactionManager.withNamespace(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return decoratedTransactionManager.getNamespace();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    decoratedTransactionManager.withTable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return decoratedTransactionManager.getTable();
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    return decoratedTransactionManager.begin();
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    return decoratedTransactionManager.begin(txId);
  }

  @Override
  public DistributedTransaction start() throws TransactionException {
    return decoratedTransactionManager.start();
  }

  @Override
  public DistributedTransaction start(String txId) throws TransactionException {
    return decoratedTransactionManager.start(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return decoratedTransactionManager.start(isolation);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return decoratedTransactionManager.start(txId, isolation);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return decoratedTransactionManager.start(isolation, strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return decoratedTransactionManager.start(strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return decoratedTransactionManager.start(txId, strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return decoratedTransactionManager.start(txId, isolation, strategy);
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
    return decoratedTransactionManager.resume(txId);
  }

  @Override
  public DistributedTransaction join(String txId) throws TransactionNotFoundException {
    return decoratedTransactionManager.join(txId);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return decoratedTransactionManager.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return decoratedTransactionManager.scan(scan);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.put(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.put(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.insert(insert);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.update(update);
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.delete(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    decoratedTransactionManager.mutate(mutations);
  }

  @Override
  public TransactionState getState(String txId) throws TransactionException {
    return decoratedTransactionManager.getState(txId);
  }

  @Override
  public TransactionState rollback(String txId) throws TransactionException {
    return decoratedTransactionManager.rollback(txId);
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return decoratedTransactionManager.abort(txId);
  }

  @Override
  public void close() {
    decoratedTransactionManager.close();
  }

  public DistributedTransactionManager getOriginalTransactionManager() {
    if (decoratedTransactionManager instanceof DecoratedDistributedTransactionManager) {
      return ((DecoratedDistributedTransactionManager) decoratedTransactionManager)
          .getOriginalTransactionManager();
    }
    return decoratedTransactionManager;
  }
}
