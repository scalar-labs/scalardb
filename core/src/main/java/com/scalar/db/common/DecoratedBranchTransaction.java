package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.Delete;
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
 * A {@link BranchTransaction} that forwards every method to a wrapped branch transaction.
 *
 * <p>Base class for branch-transaction decorators: subclasses override only the methods they need
 * and inherit plain delegation for the rest.
 */
public abstract class DecoratedBranchTransaction implements BranchTransaction {

  private final BranchTransaction branchTransaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedBranchTransaction(BranchTransaction branchTransaction) {
    this.branchTransaction = branchTransaction;
  }

  @Override
  public String getId() {
    return branchTransaction.getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return branchTransaction.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return branchTransaction.scan(scan);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return branchTransaction.getScanner(scan);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    branchTransaction.put(put);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    branchTransaction.put(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    branchTransaction.insert(insert);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    branchTransaction.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException {
    branchTransaction.update(update);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    branchTransaction.delete(delete);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    branchTransaction.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    branchTransaction.mutate(mutations);
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    return branchTransaction.batch(operations);
  }

  @Override
  public void end() throws CrudException {
    branchTransaction.end();
  }
}
