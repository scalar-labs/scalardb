package com.scalar.db.storage.jdbc;

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
