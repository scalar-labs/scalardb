package com.scalar.db.storage.jdbc;

class RdbEngineMariaDB extends RdbEngineMysql {

  @Override
  public String adjustJdbcUrl(String jdbcUrl) {
    return jdbcUrl;
  }
}
