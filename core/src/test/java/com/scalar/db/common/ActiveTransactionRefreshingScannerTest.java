package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ActiveTransactionRefreshingScannerTest {

  private static final String TX = "tx-1";

  @Test
  void one_ShouldRefreshIdleTimerThenDelegate() throws Exception {
    ActiveTransactionRegistry<?> registry = mock(ActiveTransactionRegistry.class);
    Scanner delegate = mock(Scanner.class);
    when(delegate.one()).thenReturn(Optional.empty());
    ActiveTransactionRefreshingScanner scanner =
        new ActiveTransactionRefreshingScanner(registry, TX, delegate);

    scanner.one();

    verify(registry).touch(TX);
    verify(delegate).one();
  }

  @Test
  void all_ShouldRefreshIdleTimerThenDelegate() throws Exception {
    ActiveTransactionRegistry<?> registry = mock(ActiveTransactionRegistry.class);
    Scanner delegate = mock(Scanner.class);
    when(delegate.all()).thenReturn(Collections.emptyList());
    ActiveTransactionRefreshingScanner scanner =
        new ActiveTransactionRefreshingScanner(registry, TX, delegate);

    scanner.all();

    verify(registry).touch(TX);
    verify(delegate).all();
  }

  @Test
  void close_ShouldDelegateWithoutRefreshing() throws Exception {
    ActiveTransactionRegistry<?> registry = mock(ActiveTransactionRegistry.class);
    Scanner delegate = mock(Scanner.class);
    ActiveTransactionRefreshingScanner scanner =
        new ActiveTransactionRefreshingScanner(registry, TX, delegate);

    scanner.close();

    verify(registry, never()).touch(TX);
    verify(delegate).close();
  }

  @Test
  void iterator_ShouldRefreshIdleTimerAsItAdvances() throws Exception {
    ActiveTransactionRegistry<?> registry = mock(ActiveTransactionRegistry.class);
    Scanner delegate = mock(Scanner.class);
    Result r1 = mock(Result.class);
    Result r2 = mock(Result.class);
    // Iteration is driven through this decorator's one(), so the timer is refreshed on each
    // advance.
    when(delegate.one())
        .thenReturn(Optional.of(r1))
        .thenReturn(Optional.of(r2))
        .thenReturn(Optional.empty());
    ActiveTransactionRefreshingScanner scanner =
        new ActiveTransactionRefreshingScanner(registry, TX, delegate);

    List<Result> results = new ArrayList<>();
    scanner.iterator().forEachRemaining(results::add);

    assertThat(results).containsExactly(r1, r2);
    // Refreshed once per one() call: two elements plus the terminating empty read = exactly three.
    verify(registry, times(3)).touch(TX);
  }
}
