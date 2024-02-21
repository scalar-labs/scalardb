package com.scalar.db.util.groupcommit;

@FunctionalInterface
public interface ThrowableRunnable {
  void run() throws Exception;
}
