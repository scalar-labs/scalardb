package com.scalar.db.storage.dynamo;

import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class DynamoConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    return super.getOperatorAndDataTypeListForTest().stream()
        .filter(
            operatorAndDataType -> {
              // DynamoDB only supports the 'equal' and 'not equal' and 'is null' and 'is not null'
              // conditions for BOOLEAN type
              if (operatorAndDataType.getDataType() == DataType.BOOLEAN) {
                return operatorAndDataType.getOperator() == Operator.EQ
                    || operatorAndDataType.getOperator() == Operator.NE
                    || operatorAndDataType.getOperator() == Operator.IS_NULL
                    || operatorAndDataType.getOperator() == Operator.IS_NOT_NULL;
              }
              return true;
            })
        .collect(Collectors.toList());
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getRandomDynamoDoubleColumn(random, columnName);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected boolean shouldMutate(
      Column<?> initialColumn, Column<?> columnToCompare, Operator operator) {
    switch (operator) {
      case EQ:
        return Ordering.natural().compare(initialColumn, columnToCompare) == 0;
      case NE:
        return Ordering.natural().compare(initialColumn, columnToCompare) != 0;
      default:
        return super.shouldMutate(initialColumn, columnToCompare, operator);
    }
  }
}
