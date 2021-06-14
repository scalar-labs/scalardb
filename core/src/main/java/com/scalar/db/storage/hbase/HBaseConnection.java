package com.scalar.db.storage.hbase;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;

@Immutable
public class HBaseConnection {

  private final String jdbcUrl;
  private final Properties jdbcInfo;

  public HBaseConnection(String jdbcUrl) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl);
    jdbcInfo = new Properties();
    jdbcInfo.put("phoenix.schema.isNamespaceMappingEnabled", "true");
  }

  public PhoenixConnection getConnection() throws SQLException {
    PhoenixConnection connection =
        (PhoenixConnection) PhoenixDriver.INSTANCE.connect(jdbcUrl, jdbcInfo);
    connection.setAutoCommit(true);
    return connection;
  }
}
