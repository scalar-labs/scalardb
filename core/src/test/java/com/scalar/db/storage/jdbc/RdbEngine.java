package com.scalar.db.storage.jdbc;

/**
 * Test-only enum mainly used like `@EnumSource(RdbEngine.class)`. Branching by this enum leads to
 * low cohesion, so use {@link RdbEngineFactory} to create {@link RdbEngineStrategy} subclasses
 * instead.
 */
public enum RdbEngine {
  MYSQL,
  POSTGRESQL,
  ORACLE,
  SQL_SERVER;

  public static RdbEngineStrategy createRdbEngineStrategy(RdbEngine rdbEngine) {
    switch (rdbEngine) {
      case MYSQL:
        return new RdbEngineMysql();
      case POSTGRESQL:
        return new RdbEnginePostgresql();
      case ORACLE:
        return new RdbEngineOracle();
      case SQL_SERVER:
        return new RdbEngineSqlServer();
      default:
        throw new AssertionError();
    }
  }
}
