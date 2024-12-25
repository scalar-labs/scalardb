package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class JdbcDatabaseCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  private RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected Column<?> getRandomColumn(Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
    }
    return super.getRandomColumn(random, columnName, dataType);
  }

  @Override
  protected boolean isParallelDdlSupported() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      return false;
    }
    return super.isParallelDdlSupported();
  }

  @Override
  protected Stream<Arguments> provideColumnsForCNFConditionsTest() {
    List<String> allColumnNames =
        prepareNonKeyColumns(0).stream().map(Column::getName).collect(Collectors.toList());

    if ((JdbcTestUtils.isOracle(rdbEngine)
        || JdbcTestUtils.isSqlServer(rdbEngine)
        || JdbcTestUtils.isSqlite(rdbEngine))) {
      // Oracle, SQLServer and SQLite do not support having too many conditions as CNF because it
      // is converted internally to a query with conditions in DNF which can be too large for the
      // storage to process.
      // So we split the columns into two parts randomly to split the test into two executions
      Collections.shuffle(allColumnNames, random.get());
      return Stream.of(
          Arguments.of(allColumnNames.subList(0, allColumnNames.size() / 2)),
          Arguments.of(allColumnNames.subList(allColumnNames.size() / 2, allColumnNames.size())));
    } else {
      return Stream.of(Arguments.of(allColumnNames));
    }
  }
}
