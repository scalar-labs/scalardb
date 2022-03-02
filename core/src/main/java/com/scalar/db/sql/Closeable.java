package com.scalar.db.sql;

public interface Closeable extends AutoCloseable {

  @Override
  void close();
}
