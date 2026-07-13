package com.scalar.db.common;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link Iterator} over a {@link CrudOperable.Scanner} that drives the scanner's {@code one()}
 * for every element.
 *
 * <p>Scanner decorators return this from {@code iterator()} so that iteration flows through the
 * (decorated) {@code one()} — inheriting whatever that {@code one()} does (e.g. locking, a liveness
 * re-check, or an idle-timer refresh) — rather than the underlying scanner's own iterator, which
 * would bypass the decorator. It is single-consumer and not thread-safe; any synchronization it
 * appears to provide actually lives in the {@code one()} it drives, not here.
 *
 * @param <E> the checked exception the scanner's read methods throw
 */
@NotThreadSafe
public class ScannerIterator<E extends TransactionException> implements Iterator<Result> {

  private final CrudOperable.Scanner<E> scanner;
  private Result next;

  public ScannerIterator(CrudOperable.Scanner<E> scanner) {
    this.scanner = Objects.requireNonNull(scanner);
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }

    try {
      return (next = scanner.one().orElse(null)) != null;
    } catch (TransactionException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Result ret = next;
    next = null;
    return ret;
  }
}
