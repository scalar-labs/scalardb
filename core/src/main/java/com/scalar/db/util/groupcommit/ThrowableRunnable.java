package com.scalar.db.util.groupcommit;

@FunctionalInterface
interface ThrowableRunnable {
  void run() throws Exception;
}
