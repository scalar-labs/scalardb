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
  public int getMaximumIsolationLevel() {
    // TiDB doesn't support SERIALIZABLE isolation level
    return Connection.TRANSACTION_REPEATABLE_READ;
  }
}
