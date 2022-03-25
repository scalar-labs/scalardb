package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageConditionalMutationIntegrationTestBase;
import com.scalar.db.storage.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JdbcDatabaseConditionalMutationIntegrationTest
    extends StorageConditionalMutationIntegrationTestBase {

  private static RdbEngine rdbEngine;

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    JdbcConfig jdbcConfig = JdbcEnv.getJdbcConfig();
    rdbEngine = JdbcUtils.getRdbEngine(jdbcConfig.getContactPoints().get(0));
    return jdbcConfig;
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    if (rdbEngine == RdbEngine.ORACLE) {
      List<OperatorAndDataType> ret = new ArrayList<>();
      for (Operator operator : Operator.values()) {
        for (DataType dataType : DataType.values()) {
          // Oracle doesn't support conditions for BLOB type
          if (dataType == DataType.BLOB) {
            continue;
          }
          ret.add(new OperatorAndDataType(operator, dataType));
        }
      }
      return ret;
    }

    return super.getOperatorAndDataTypeListForTest();
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleColumn(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }
}
