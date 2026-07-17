package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;

/**
 * Adapts a {@link DistributedTransaction} to the {@link BranchTransaction} API.
 *
 * <p>This is a branch of a single-phase-backed distributed transaction. CRUD is delegated directly
 * to the underlying {@link DistributedTransaction} (which this branch joined by the global
 * transaction's ID); the commit/rollback outcome is driven by the owning {@link
 * com.scalar.db.api.GlobalTransaction}. {@link #end()} is a no-op for this backing.
 *
 * <p>Operations must be fully qualified with their namespace and table; this handle carries no
 * default target. See {@link DistributedTransactionBackedGlobalTransactionManager} for how the
 * transaction is wired and the branch is begun.
 */
public class DistributedTransactionBackedBranchTransaction implements BranchTransaction {

  private final DistributedTransaction transaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DistributedTransactionBackedBranchTransaction(DistributedTransaction transaction) {
    this.transaction = transaction;
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

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    transaction.put(put);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    transaction.put(puts);
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
  public void delete(Delete delete) throws CrudException {
    transaction.delete(delete);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    transaction.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    transaction.mutate(mutations);
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    return transaction.batch(operations);
  }

  @Override
  public void end() throws CrudException {}
}
