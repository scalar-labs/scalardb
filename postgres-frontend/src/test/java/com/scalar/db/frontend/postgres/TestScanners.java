package com.scalar.db.frontend.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionManagerCrudOperable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Makes a mocked {@code getScanner} serve whatever the mock's {@code scan} is stubbed to return.
 */
final class TestScanners {
  private TestScanners() {}

  static void stub(DistributedTransaction transaction) throws Exception {
    when(transaction.getScanner(any()))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              return new TransactionScanner(transaction.scan(scan).iterator());
            });
  }

  static void stub(DistributedTransactionManager manager) throws Exception {
    when(manager.getScanner(any()))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              return new ManagerScanner(manager.scan(scan).iterator());
            });
  }

  private static List<Result> remaining(Iterator<Result> results) {
    List<Result> out = new ArrayList<>();
    results.forEachRemaining(out::add);
    return out;
  }

  private static final class TransactionScanner implements TransactionCrudOperable.Scanner {
    private final Iterator<Result> results;

    TransactionScanner(Iterator<Result> results) {
      this.results = results;
    }

    @Override
    public Optional<Result> one() {
      return results.hasNext() ? Optional.of(results.next()) : Optional.empty();
    }

    @Override
    public List<Result> all() {
      return remaining(results);
    }

    @Override
    public void close() {}

    @Override
    public Iterator<Result> iterator() {
      return results;
    }
  }

  private static final class ManagerScanner implements TransactionManagerCrudOperable.Scanner {
    private final Iterator<Result> results;

    ManagerScanner(Iterator<Result> results) {
      this.results = results;
    }

    @Override
    public Optional<Result> one() {
      return results.hasNext() ? Optional.of(results.next()) : Optional.empty();
    }

    @Override
    public List<Result> all() {
      return remaining(results);
    }

    @Override
    public void close() {}

    @Override
    public Iterator<Result> iterator() {
      return results;
    }
  }
}
