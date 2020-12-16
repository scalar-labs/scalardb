package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.JDBCUtils;

public class RDBInfo {
  public final String jdbcJrl;
  public final String username;
  public final String password;

  public RDBInfo(String jdbcJrl, String username, String password) {
    this.jdbcJrl = jdbcJrl;
    this.username = username;
    this.password = password;
  }

  @Override
  public String toString() {
    return JDBCUtils.getRDBType(jdbcJrl).toString();
  }
}
