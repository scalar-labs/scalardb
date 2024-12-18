package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import java.sql.Driver;
import java.sql.JDBCType;

class RdbEngineMariaDB extends RdbEngineMysql {
  @Override
  public Driver getDriver() {
    return new org.mariadb.jdbc.Driver();
  }

  @Override
  public DataType getDataTypeForScalarDb(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
    if (type == JDBCType.BOOLEAN) {
      // MariaDB JDBC driver maps TINYINT(1) type as a BOOLEAN JDBC type which differs from the
      // MySQL driver which maps it to a BIT type.
      return DataType.BOOLEAN;
    } else {
      return super.getDataTypeForScalarDb(type, typeName, columnSize, digits, columnDescription);
    }
  }
}
