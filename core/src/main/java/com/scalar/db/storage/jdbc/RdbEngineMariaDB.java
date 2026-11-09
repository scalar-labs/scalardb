package com.scalar.db.storage.jdbc;

class RdbEngineMariaDB extends RdbEngineMysql {

  // Every supported MariaDB version has RENAME COLUMN, which the 10.5 series introduced.
  RdbEngineMariaDB() {
    super(true);
  }

  @Override
  public String getBinaryTextCollationClause() {
    return "CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin";
  }

  @Override
  public String adjustJdbcUrl(String jdbcUrl) {
    return jdbcUrl;
  }
}
