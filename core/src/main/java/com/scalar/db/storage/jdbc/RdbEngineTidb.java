package com.scalar.db.storage.jdbc;

import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This implements a RdbEngine for TiDB that extends MySQL one. TiDB is MySQL compatible and uses
 * the same connection string, so special handling is needed to instantiate it, cf. {@link
 * RdbEngineFactory#create(JdbcConfig)}
 */
public class RdbEngineTidb extends RdbEngineMysql {

  private static final Pattern TIDB_VERSION_PATTERN = Pattern.compile("TiDB-v(\\d+)\\.(\\d+)");

  RdbEngineTidb(JdbcConfig config) {
    this(config, "utf8mb4_bin");
  }

  RdbEngineTidb(JdbcConfig config, String binaryCollation) {
    super(config, false, true, binaryCollation);
  }

  /** Returns the binary utf8mb4 collation to declare on TEXT columns. NO PAD arrives in 7.4. */
  static String selectTidbBinaryCollation(String productVersion) {
    Matcher matcher = TIDB_VERSION_PATTERN.matcher(productVersion);
    if (!matcher.find()) {
      return "utf8mb4_bin";
    }
    try {
      int major = Integer.parseInt(matcher.group(1));
      int minor = Integer.parseInt(matcher.group(2));
      boolean supported = major > 7 || (major == 7 && minor >= 4);
      return supported ? "utf8mb4_0900_bin" : "utf8mb4_bin";
    } catch (NumberFormatException e) {
      return "utf8mb4_bin";
    }
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
}
