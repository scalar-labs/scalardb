package com.scalar.db.common;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Iterator;
import javax.annotation.Nonnull;

public abstract class AbstractCrudOperableScanner<E extends TransactionException>
    implements CrudOperable.Scanner<E> {

  @LazyInit private ScannerIterator<E> scannerIterator;

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator<>(this);
    }
    return scannerIterator;
  }
}
