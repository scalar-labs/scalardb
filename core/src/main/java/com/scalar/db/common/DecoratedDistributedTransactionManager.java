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
import java.util.function.BiConsumer;

public abstract class DecoratedDistributedTransactionManager
    implements DistributedTransactionManager, DistributedTransactionExpirationHandlerSettable {

  private final DistributedTransactionManager transactionManager;

  public DecoratedDistributedTransactionManager(DistributedTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    transactionManager.with(namespace, tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    transactionManager.withNamespace(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return transactionManager.getNamespace();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    transactionManager.withTable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return transactionManager.getTable();
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.begin());
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.begin(txId));
  }

  @Override
  public DistributedTransaction start() throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start());
  }

  @Override
  public DistributedTransaction start(String txId) throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(txId));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(isolation));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(txId, isolation));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(isolation, strategy));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(strategy));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(txId, strategy));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return decorateTransactionOnBeginOrStart(transactionManager.start(txId, isolation, strategy));
  }

  protected DistributedTransaction decorateTransactionOnBeginOrStart(
      DistributedTransaction transaction) throws TransactionException {
    return transaction;
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
    return transactionManager.resume(txId);
  }

  @Override
  public DistributedTransaction join(String txId) throws TransactionNotFoundException {
    return transactionManager.join(txId);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return transactionManager.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return transactionManager.scan(scan);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return transactionManager.getScanner(scan);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    transactionManager.put(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    transactionManager.put(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    transactionManager.insert(insert);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    transactionManager.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    transactionManager.update(update);
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    transactionManager.delete(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    transactionManager.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    transactionManager.mutate(mutations);
  }

  @Override
  public TransactionState getState(String txId) throws TransactionException {
    return transactionManager.getState(txId);
  }

  @Override
  public TransactionState rollback(String txId) throws TransactionException {
    return transactionManager.rollback(txId);
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return transactionManager.abort(txId);
  }

  @Override
  public void close() {
    transactionManager.close();
  }

  public DistributedTransactionManager getOriginalTransactionManager() {
    if (transactionManager instanceof DecoratedDistributedTransactionManager) {
      return ((DecoratedDistributedTransactionManager) transactionManager)
          .getOriginalTransactionManager();
    }
    return transactionManager;
  }

  @Override
  public void setTransactionExpirationHandler(BiConsumer<String, DistributedTransaction> handler) {
    if (transactionManager instanceof DistributedTransactionExpirationHandlerSettable) {
      ((DistributedTransactionExpirationHandlerSettable) transactionManager)
          .setTransactionExpirationHandler(handler);
    }
  }
}
