package com.scalar.db.storage.jdbc;

import java.sql.Driver;

class RdbEngineYugabyte extends RdbEnginePostgresql {
  @Override
  public Driver getDriver() {
    return new com.yugabyte.Driver();
  }
}
