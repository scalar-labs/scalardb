package com.scalar.db.util;

@FunctionalInterface
public interface ThrowableSupplier<R, T extends Throwable> {
  R get() throws T;
}
