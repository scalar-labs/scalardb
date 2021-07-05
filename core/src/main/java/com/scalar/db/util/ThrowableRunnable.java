package com.scalar.db.util;

@FunctionalInterface
public interface ThrowableRunnable<T extends Throwable> {
  void run() throws T;
}
