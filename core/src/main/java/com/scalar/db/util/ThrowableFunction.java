package com.scalar.db.util;

@FunctionalInterface
public interface ThrowableFunction<A, R, T extends Throwable> {
  R apply(A arg) throws T;
}
