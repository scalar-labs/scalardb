package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class JdbcDatabaseColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithMinValue(columnName, dataType, false);
    }
    if (JdbcTestUtils.isDb2(rdbEngine)) {
      if (dataType == DataType.FLOAT) {
        return JdbcTestUtils.getMinDb2FloatValue(columnName);
      }
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinDb2DoubleValue(columnName);
      }
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMaxOracleDoubleValue(columnName);
      }
    }
    if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      if (dataType == DataType.TEXT) {
        return JdbcTestUtils.getMaxSqlServerTextValue(columnName);
      }
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }

  protected Stream<Arguments> provideBlobSizes() {
    List<Arguments> args = new ArrayList<>();
    if (JdbcEnv.isOracle()) {
      // As explained in
      // `com.scalar.db.storage.jdbc.RdbEngineOracle.bindBlobColumnToPreparedStatement()`,
      // handing a BLOB size bigger than 32,766 bytes requires a workaround so we particularly test
      // values around it.
      args.add(Arguments.of(32_766, "32.766 KB"));
      args.add(Arguments.of(32_767, "32.767 KB"));
    }
    args.add(Arguments.of(100_000_000, "100 MB"));
    return args.stream();
  }
}
