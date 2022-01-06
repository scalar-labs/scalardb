package com.scalar.db.storage.jdbc;

public enum Isolation {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE
}
