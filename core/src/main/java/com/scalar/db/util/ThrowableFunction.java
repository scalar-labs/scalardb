package com.scalar.db.util;

@FunctionalInterface
public interface ThrowableFunction<R, A, T extends Throwable> {
  R apply(A arg) throws T;
}
