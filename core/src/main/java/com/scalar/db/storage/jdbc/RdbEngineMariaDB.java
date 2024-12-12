package com.scalar.db.storage.jdbc;

import java.sql.Driver;

class RdbEngineMariaDB extends RdbEngineMysql {
  @Override
  public Driver getDriver() {
    return new org.mariadb.jdbc.Driver();
  }
}
