package com.scalar.db.common;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link DecoratedBranchTransaction} that propagates the per-branch attributes supplied at {@link
 * GlobalTransactionManager#beginBranch(String, Map)} into every CRUD operation issued on the
 * branch.
 *
 * <p>The attributes are merged into each operation before delegating, with an attribute set
 * directly on the operation winning over the per-branch one (see {@link OperationAttributeMerger}).
 * These per-branch attributes are held client-side and differ per branch; they are distinct from
 * the transaction-scoped attributes supplied at {@code beginGlobal}, which the backing propagates
 * separately (on the participant side for the two-phase-commit backing).
 *
 * <p>Since this decorator merges before the backing propagates the transaction-scoped attributes,
 * and merging never overwrites an attribute already present on the operation, the effective
 * precedence for a key supplied at more than one scope is: the attribute set directly on the
 * operation, then the per-branch attribute supplied at {@code beginBranch}, then the
 * transaction-scoped attribute supplied at {@code beginGlobal}. The narrower scope takes
 * precedence.
 */
@NotThreadSafe
public class AttributePropagatingBranchTransaction extends DecoratedBranchTransaction {

  private final Map<String, String> attributes;

  public AttributePropagatingBranchTransaction(
      BranchTransaction branchTransaction, Map<String, String> attributes) {
    super(branchTransaction);
    this.attributes = ImmutableMap.copyOf(attributes);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return super.get(merge(get));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return super.scan(merge(scan));
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return super.getScanner(merge(scan));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    super.put(merge(put));
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    super.put(mergeEach(puts));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    super.insert(merge(insert));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    super.upsert(merge(upsert));
  }

  @Override
  public void update(Update update) throws CrudException {
    super.update(merge(update));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    super.delete(merge(delete));
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    super.delete(mergeEach(deletes));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    super.mutate(mergeEach(mutations));
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    return super.batch(mergeEach(operations));
  }

  private <T extends Operation> T merge(T operation) {
    return OperationAttributeMerger.merge(operation, attributes);
  }

  private <T extends Operation> List<T> mergeEach(List<? extends T> operations) {
    return OperationAttributeMerger.mergeEach(operations, attributes);
  }
}
