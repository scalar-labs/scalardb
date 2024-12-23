package com.scalar.db.storage.jdbc;

import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.TestUtils;
import java.util.Random;
import java.util.stream.IntStream;

public final class JdbcTestUtils {

  public static final double MAX_ORACLE_DOUBLE_VALUE = 9.99999999999999E125D;
  public static final double MIN_ORACLE_DOUBLE_VALUE = -9.99999999999999E125D;

  private JdbcTestUtils() {}

  public static Column<?> getRandomOracleDoubleValue(Random random, String columnName) {
    return DoubleColumn.of(columnName, nextOracleDouble(random));
  }

  public static Column<?> getRandomOracleDoubleColumn(Random random, String columnName) {
    return DoubleColumn.of(columnName, nextOracleDouble(random));
  }

  public static double nextOracleDouble(Random random) {
    return random
        .doubles(1, MIN_ORACLE_DOUBLE_VALUE, MAX_ORACLE_DOUBLE_VALUE)
        .findFirst()
        .orElse(0.0d);
  }

  public static DoubleColumn getMinOracleDoubleValue(String columnName) {
    return DoubleColumn.of(columnName, MIN_ORACLE_DOUBLE_VALUE);
  }

  public static DoubleColumn getMaxOracleDoubleValue(String columnName) {
    return DoubleColumn.of(columnName, MAX_ORACLE_DOUBLE_VALUE);
  }

  public static TextColumn getMaxSqlServerTextValue(String columnName) {
    // Since SQL Server can't handle 0xFF character correctly, we use "ZZZ..." as the max value
    StringBuilder builder = new StringBuilder();
    IntStream.range(0, TestUtils.MAX_TEXT_COUNT).forEach(i -> builder.append('Z'));
    return TextColumn.of(columnName, builder.toString());
  }

  public static boolean isPostgresql(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEnginePostgresql;
  }

  public static boolean isMysql(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEngineMysql;
  }

  public static boolean isOracle(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEngineOracle;
  }

  public static boolean isSqlServer(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEngineSqlServer;
  }

  public static boolean isSqlite(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEngineSqlite;
  }

  public static boolean isYugabyte(RdbEngineStrategy rdbEngine) {
    return rdbEngine instanceof RdbEngineYugabyte;
  }
}
