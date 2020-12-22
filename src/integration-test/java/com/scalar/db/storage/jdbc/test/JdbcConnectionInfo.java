package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.JdbcUtils;

public class JdbcConnectionInfo {
  public final String url;
  public final String username;
  public final String password;

  public JdbcConnectionInfo(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
  }

  @Override
  public String toString() {
    return JdbcUtils.getRdbEngine(url).toString();
  }
}
