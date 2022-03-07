package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageConditionalMutationIntegrationTestBase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DynamoConditionalMutationIntegrationTest
    extends StorageConditionalMutationIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    List<OperatorAndDataType> ret = new ArrayList<>();
    for (Operator operator : Operator.values()) {
      for (DataType dataType : DataType.values()) {
        // DynamoDB only supports the 'equal' and 'not equal' conditions for BOOLEAN type
        if (dataType == DataType.BOOLEAN) {
          if (operator == Operator.EQ || operator == Operator.NE) {
            ret.add(new OperatorAndDataType(operator, dataType));
          }
        } else {
          ret.add(new OperatorAndDataType(operator, dataType));
        }
      }
    }
    return ret;
  }

  @Override
  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getRandomDynamoDoubleValue(random, columnName);
    }
    return super.getRandomValue(random, columnName, dataType);
  }
}
