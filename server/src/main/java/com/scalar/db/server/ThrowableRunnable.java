package com.scalar.db.server;

@FunctionalInterface
public interface ThrowableRunnable<T extends Throwable> {
  void run() throws T;
}
