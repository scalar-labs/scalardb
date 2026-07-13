package com.scalar.db.common;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import com.scalar.db.exception.transaction.CrudException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * A {@link Scanner} decorator that refreshes a transaction's idle timer in its {@link
 * ActiveTransactionRegistry} on every read, so that consuming a long-lived scanner counts as
 * activity.
 *
 * <p>Active-transaction tracking refreshes a transaction's idle timer on each CRUD/record call, but
 * a scanner outlives the {@code getScanner} call: its {@code one()} / {@code all()} run afterwards
 * (for a remote participant, across separate pull RPCs). Without this wrapper those reads would not
 * refresh the timer, so a transaction streaming a large result slowly could be reaped mid-scan.
 *
 * <p>{@code iterator()} drives iteration through this decorator's {@code one()}, so each advance
 * refreshes the timer (and, via the delegate's {@code one()}, inherits whatever the delegate does —
 * e.g. synchronization in {@link SynchronizedScanner}). {@code close()} is not treated as activity
 * and does not refresh the timer.
 *
 * <p>Only actual reads refresh the timer: merely holding the returned scanner without calling
 * {@code one()} / {@code all()} (or iterating) does not count as activity, so an unread, idle
 * scanner can still be reaped once its transaction's idle window elapses.
 */
class ActiveTransactionRefreshingScanner implements Scanner {

  private final ActiveTransactionRegistry<?> registry;
  private final String transactionId;
  private final Scanner delegate;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  ActiveTransactionRefreshingScanner(
      ActiveTransactionRegistry<?> registry, String transactionId, Scanner delegate) {
    this.registry = registry;
    this.transactionId = transactionId;
    this.delegate = delegate;
  }

  @Override
  public Optional<Result> one() throws CrudException {
    registry.touch(transactionId);
    return delegate.one();
  }

  @Override
  public List<Result> all() throws CrudException {
    registry.touch(transactionId);
    return delegate.all();
  }

  @Override
  public void close() throws CrudException {
    delegate.close();
  }

  @Nonnull
  @Override
  public Iterator<Result> iterator() {
    // Drive iteration through this decorator's one(), which refreshes the idle timer (and, via the
    // delegate's one(), inherits whatever the delegate does, e.g. synchronization).
    return new ScannerIterator<>(this);
  }
}
