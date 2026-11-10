package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
  public String importColumnAttributesSql() {
    // In addition to the EXTRA column that RdbEngineMysql checks, get whether each column is an
    // AUTO_RANDOM column. TiDB does not show the AUTO_RANDOM attribute in the EXTRA column, but
    // TIDB_ROW_ID_SHARDING_INFO of INFORMATION_SCHEMA.TABLES is "PK_AUTO_RANDOM_BITS=<n>" for a
    // table with an AUTO_RANDOM column, which is always the first column of the primary key. Note
    // that some TiDB versions (e.g., 6.5 and 7.5) show that value only for a table with a
    // single-column primary key, so an AUTO_RANDOM column in a composite primary key is not
    // detected in those versions
    return "SELECT c.COLUMN_NAME, c.EXTRA,"
        + " CASE WHEN k.ORDINAL_POSITION = 1"
        + " AND t.TIDB_ROW_ID_SHARDING_INFO LIKE 'PK_AUTO_RANDOM_BITS%' THEN 1 ELSE 0 END"
        + " FROM INFORMATION_SCHEMA.COLUMNS c"
        + " JOIN INFORMATION_SCHEMA.TABLES t"
        + " ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME"
        + " LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k"
        + " ON k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME"
        + " AND k.COLUMN_NAME = c.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'"
        + " WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?";
  }

  @Override
  public void validateImportColumnAttributes(ResultSet resultSet, String namespace, String table)
      throws SQLException {
    super.validateImportColumnAttributes(resultSet, namespace, table);

    // We treat an AUTO_RANDOM column as an auto-increment column. TiDB rejects a value specified
    // for an AUTO_RANDOM column by default, and replaces 0 with a generated value even if the
    // allow_auto_random_explicit_insert system variable is enabled
    if (resultSet.getInt(3) == 1) {
      throw new IllegalArgumentException(
          CoreError.JDBC_IMPORT_AUTO_INCREMENT_COLUMN_NOT_SUPPORTED.buildMessage(
              getFullTableName(namespace, table), resultSet.getString(1)));
    }
  }

  @Override
  public int getHighestIsolationLevel() {
    // TiDB doesn't support SERIALIZABLE isolation level
    return Connection.TRANSACTION_REPEATABLE_READ;
  }
}
