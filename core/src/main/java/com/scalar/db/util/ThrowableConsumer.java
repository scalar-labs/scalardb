package com.scalar.db.util;

@FunctionalInterface
public interface ThrowableConsumer<A, T extends Throwable> {
  void accept(A arg) throws T;
}
