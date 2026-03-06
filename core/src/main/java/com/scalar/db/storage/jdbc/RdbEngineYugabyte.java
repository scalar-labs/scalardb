package com.scalar.db.storage.jdbc;

class RdbEngineYugabyte extends RdbEnginePostgresql {
  @Override
  public String getDriverClassName() {
    return com.yugabyte.Driver.class.getName();
  }
}
