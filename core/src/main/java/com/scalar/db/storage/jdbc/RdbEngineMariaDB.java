package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

class RdbEngineMariaDB extends RdbEngineMysql {
  @Override
  public Driver getDriver() {
    return new org.mariadb.jdbc.Driver();
  }

  @Override
  DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    if (type == JDBCType.BOOLEAN) {
      // MariaDB JDBC driver maps TINYINT(1) type as a BOOLEAN JDBC type which differs from the
      // MySQL driver which maps it to a BIT type.
      return DataType.BOOLEAN;
    } else {
      return super.getDataTypeForScalarDbInternal(
          type, typeName, columnSize, digits, columnDescription, overrideDataType);
    }
  }

  @Override
  public Map<String, String> getConnectionProperties(JdbcConfig config) {
    return Collections.emptyMap();
  }

  @Override
  public void setConnectionToReadOnly(Connection connection, boolean readOnly) throws SQLException {
    connection.setReadOnly(readOnly);
  }
}
