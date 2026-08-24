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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Adapts a {@link DistributedTransaction} to the {@link BranchTransaction} API.
 *
 * <p>This is a branch of a single-phase-backed distributed transaction. CRUD is delegated directly
 * to the underlying {@link DistributedTransaction}, which every branch of the global transaction
 * shares (see {@link DistributedTransactionBackedGlobalTransactionManager} for the concurrency
 * implications); the commit/rollback outcome is driven by the owning {@link
 * com.scalar.db.api.GlobalTransaction}. {@link #end(BranchTransaction.Status)} triggers no backing
 * action for this backing — it only marks the branch ended, after which CRUD (or another {@code
 * end}) is rejected with {@link IllegalStateException}.
 *
 * <p>Operations must be fully qualified with their namespace and table; this handle carries no
 * default target. See {@link DistributedTransactionBackedGlobalTransactionManager} for how the
 * transaction is wired and the branch is begun.
 */
@NotThreadSafe
public class DistributedTransactionBackedBranchTransaction implements BranchTransaction {

  private final DistributedTransaction transaction;

  private boolean ended;

  // Scanners handed out and not yet closed; end(SUCCESS) refuses to run while any remain, so a
  // scanner can never legitimately outlive a successfully ended branch (mirroring the
  // scanner-not-closed guard consensus commit applies at commit). end(FAILURE) leaves them alone;
  // see its comment below.
  private final Set<Scanner> openScanners = new HashSet<>();

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
    checkNotEnded();
    return transaction.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    checkNotEnded();
    return transaction.scan(scan);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    checkNotEnded();
    return trackScanner(transaction.getScanner(scan));
  }

  private Scanner trackScanner(Scanner scanner) {
    Scanner tracked =
        new Scanner() {
          @Override
          public Optional<Result> one() throws CrudException {
            return scanner.one();
          }

          @Override
          public List<Result> all() throws CrudException {
            return scanner.all();
          }

          @Override
          public Iterator<Result> iterator() {
            return scanner.iterator();
          }

          @Override
          public void close() throws CrudException {
            // Untrack even if the close fails: the guard is about forgotten closes, and an
            // uncloseable scanner must not make the branch unendable forever.
            try {
              scanner.close();
            } finally {
              openScanners.remove(this);
            }
          }
        };
    openScanners.add(tracked);
    return tracked;
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    checkNotEnded();
    transaction.put(put);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    checkNotEnded();
    transaction.put(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    checkNotEnded();
    transaction.insert(insert);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    checkNotEnded();
    transaction.upsert(upsert);
  }

  @Override
  public void update(Update update) throws CrudException {
    checkNotEnded();
    transaction.update(update);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    checkNotEnded();
    transaction.delete(delete);
  }

  /**
   * @deprecated As of release 3.13.0. Will be removed in release 4.0.0. Use {@link #mutate(List)}.
   */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkNotEnded();
    transaction.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkNotEnded();
    transaction.mutate(mutations);
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    checkNotEnded();
    return transaction.batch(operations);
  }

  @Override
  public void end(Status status) throws CrudException {
    Objects.requireNonNull(status);

    if (status == Status.FAILURE) {
      // Lenient by design: this runs on a failure path, so it must neither throw nor mask the
      // original failure. An already-ended branch is a no-op, and an open scanner is left as-is:
      // closing it here would write into the snapshot's scan/scanner sets, which are re-validated
      // at commit, so one branch's cleanup could abort the whole transaction. A scanner left open
      // is closed by the owning transaction's rollback (or reclaimed by idle expiry).
      ended = true;
      return;
    }

    checkNotEnded();
    if (!openScanners.isEmpty()) {
      throw new IllegalStateException(
          CoreError.BRANCH_TRANSACTION_SCANNER_NOT_CLOSED.buildMessage(transaction.getId()));
    }
    ended = true;
  }

  private void checkNotEnded() {
    if (ended) {
      throw new IllegalStateException(
          CoreError.BRANCH_TRANSACTION_ALREADY_ENDED.buildMessage(transaction.getId()));
    }
  }
}
