package com.scalar.db.common;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import com.scalar.db.exception.transaction.CrudException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * A decorator that wraps a Scanner with synchronized access for thread safety.
 *
 * <p>This class is used to ensure thread-safe access to a Scanner instance when multiple threads
 * may access it concurrently, particularly in the context of active transaction management where
 * the rollback() method may be called from an expiration handler in a different thread.
 */
public class SynchronizedScanner implements Scanner {

  private final Object lock;
  private final Scanner delegate;

  /**
   * Constructs a SynchronizedScanner.
   *
   * @param lock the object to synchronize on
   * @param delegate the underlying Scanner to delegate to
   */
  public SynchronizedScanner(Object lock, Scanner delegate) {
    this.lock = lock;
    this.delegate = delegate;
  }

  @Override
  public Optional<Result> one() throws CrudException {
    synchronized (lock) {
      return delegate.one();
    }
  }

  @Override
  public List<Result> all() throws CrudException {
    synchronized (lock) {
      return delegate.all();
    }
  }

  @Override
  public void close() throws CrudException {
    synchronized (lock) {
      delegate.close();
    }
  }

  @Nonnull
  @Override
  public Iterator<Result> iterator() {
    synchronized (lock) {
      return delegate.iterator();
    }
  }
}
