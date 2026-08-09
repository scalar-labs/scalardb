package com.scalar.db.storage.jdbc;

import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import java.sql.Connection;

/**
 * This implements a RdbEngine for TiDB that extends MySQL one. TiDB is MySQL compatible and uses
 * the same connection string, so special handling is needed to instantiate it, cf. {@link
 * RdbEngineFactory#create(JdbcConfig)}
 */
public class RdbEngineTidb extends RdbEngineMysql {

  RdbEngineTidb(JdbcConfig config) {
    super(config);
  }

  @Override
  public void throwIfAlterColumnTypeNotSupported(DataType from, DataType to) {
    if (from == DataType.BLOB && to == DataType.TEXT) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_TIDB_UNSUPPORTED_COLUMN_TYPE_CONVERSION.buildMessage(
              from.toString(), to.toString()));
    }
  }

  @Override
  public int getHighestIsolationLevel() {
    // TiDB doesn't support SERIALIZABLE isolation level
    return Connection.TRANSACTION_REPEATABLE_READ;
  }

  @Override
  public String adjustJdbcUrl(String jdbcUrl) {
    // MariaDB client 3.5.10 makes setReadOnly() issue
    // SET SESSION TRANSACTION READ ONLY, which TiDB rejects unless
    // tidb_enable_noop_functions is enabled. Keep the pre-3.5.10 behavior.
    String url = super.adjustJdbcUrl(jdbcUrl);
    return url.contains("readOnlyPropagatesToServer")
        ? url
        : url + "&readOnlyPropagatesToServer=false";
  }
}
