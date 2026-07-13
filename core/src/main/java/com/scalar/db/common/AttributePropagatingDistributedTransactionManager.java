package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link DistributedTransactionManager} decorator that wraps the transactions it returns with
 * {@link AttributePropagatingDistributedTransaction} so that attributes given to any
 * attribute-taking {@code begin*} / {@code start*} variant (including read-only variants) are
 * propagated to every operation issued on the transaction.
 *
 * <p>This decorator wraps the transaction only when the attributes map is non-empty, so
 * attribute-free begin/start variants continue to return the same concrete transaction type as the
 * underlying manager.
 */
@ThreadSafe
public class AttributePropagatingDistributedTransactionManager
    extends DecoratedDistributedTransactionManager {

  public AttributePropagatingDistributedTransactionManager(
      DistributedTransactionManager transactionManager) {
    super(transactionManager);
  }

  @Override
  public DistributedTransaction begin(Map<String, String> attributes) throws TransactionException {
    return wrapIfNeeded(super.begin(attributes), attributes);
  }

  @Override
  public DistributedTransaction begin(String txId, Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.begin(txId, attributes), attributes);
  }

  @Override
  public DistributedTransaction beginReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.beginReadOnly(attributes), attributes);
  }

  @Override
  public DistributedTransaction beginReadOnly(String txId, Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.beginReadOnly(txId, attributes), attributes);
  }

  @Override
  public DistributedTransaction start(Map<String, String> attributes) throws TransactionException {
    return wrapIfNeeded(super.start(attributes), attributes);
  }

  @Override
  public DistributedTransaction start(String txId, Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.start(txId, attributes), attributes);
  }

  @Override
  public DistributedTransaction startReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.startReadOnly(attributes), attributes);
  }

  @Override
  public DistributedTransaction startReadOnly(String txId, Map<String, String> attributes)
      throws TransactionException {
    return wrapIfNeeded(super.startReadOnly(txId, attributes), attributes);
  }

  private DistributedTransaction wrapIfNeeded(
      DistributedTransaction transaction, Map<String, String> attributes) {
    if (attributes.isEmpty()) {
      return transaction;
    }
    return new AttributePropagatingDistributedTransaction(transaction, attributes);
  }

  /**
   * A {@link DistributedTransaction} decorator that propagates the transaction-scoped attributes
   * given to any attribute-taking {@code begin*} / {@code start*} variant (including read-only
   * variants) into every operation issued on the transaction. If an operation already carries an
   * attribute with the same name, the operation's value wins.
   */
  @NotThreadSafe
  @VisibleForTesting
  static class AttributePropagatingDistributedTransaction extends DecoratedDistributedTransaction {

    private final ImmutableMap<String, String> transactionAttributes;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    AttributePropagatingDistributedTransaction(
        DistributedTransaction transaction, Map<String, String> transactionAttributes) {
      super(transaction);
      this.transactionAttributes = ImmutableMap.copyOf(transactionAttributes);
    }

    @Override
    public Optional<Result> get(Get get) throws CrudException {
      return super.get(mergeAttributes(get));
    }

    @Override
    public List<Result> scan(Scan scan) throws CrudException {
      return super.scan(mergeAttributes(scan));
    }

    @Override
    public Scanner getScanner(Scan scan) throws CrudException {
      return super.getScanner(mergeAttributes(scan));
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void put(Put put) throws CrudException {
      super.put(mergeAttributes(put));
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void put(List<Put> puts) throws CrudException {
      super.put(mergeAttributesForEach(puts));
    }

    @Override
    public void insert(Insert insert) throws CrudException {
      super.insert(mergeAttributes(insert));
    }

    @Override
    public void upsert(Upsert upsert) throws CrudException {
      super.upsert(mergeAttributes(upsert));
    }

    @Override
    public void update(Update update) throws CrudException {
      super.update(mergeAttributes(update));
    }

    @Override
    public void delete(Delete delete) throws CrudException {
      super.delete(mergeAttributes(delete));
    }

    /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
    @Deprecated
    @Override
    public void delete(List<Delete> deletes) throws CrudException {
      super.delete(mergeAttributesForEach(deletes));
    }

    @Override
    public void mutate(List<? extends Mutation> mutations) throws CrudException {
      super.mutate(this.<Mutation>mergeAttributesForEach(mutations));
    }

    @Override
    public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
      return super.batch(this.<Operation>mergeAttributesForEach(operations));
    }

    private <T extends Operation> List<T> mergeAttributesForEach(List<? extends T> operations) {
      return OperationAttributeMerger.mergeEach(operations, transactionAttributes);
    }

    private <T extends Operation> T mergeAttributes(T operation) {
      return OperationAttributeMerger.merge(operation, transactionAttributes);
    }
  }
}
