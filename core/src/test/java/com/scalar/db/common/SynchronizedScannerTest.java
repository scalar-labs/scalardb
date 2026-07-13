package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import java.util.Iterator;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SynchronizedScannerTest {

  @Test
  void iterator_ShouldDriveDelegateOneAndNotDelegateIterator() throws Exception {
    // Arrange — the delegate yields one result then exhausts.
    Scanner delegate = mock(Scanner.class);
    Result result = mock(Result.class);
    when(delegate.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    SynchronizedScanner scanner = new SynchronizedScanner(new Object(), delegate);

    // Act — iterate.
    Iterator<Result> iterator = scanner.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isSameAs(result);
    assertThat(iterator.hasNext()).isFalse();

    // Assert — iteration is routed through the synchronized one() (so it inherits the lock), and
    // the
    // delegate's own (unsynchronized) iterator is never used.
    verify(delegate, never()).iterator();
  }
}
