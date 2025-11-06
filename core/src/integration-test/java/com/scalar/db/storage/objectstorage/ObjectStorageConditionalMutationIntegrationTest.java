package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ObjectStorageConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    return super.getOperatorAndDataTypeListForTest().stream()
        .filter(
            operatorAndDataType -> {
              // Object Storage only supports EQ, NE, IS_NULL, and IS_NOT_NULL conditions for BLOB
              // type
              if (operatorAndDataType.getDataType() == DataType.BLOB) {
                return operatorAndDataType.getOperator() == ConditionalExpression.Operator.EQ
                    || operatorAndDataType.getOperator() == ConditionalExpression.Operator.NE
                    || operatorAndDataType.getOperator() == ConditionalExpression.Operator.IS_NULL
                    || operatorAndDataType.getOperator()
                        == ConditionalExpression.Operator.IS_NOT_NULL;
              }
              return true;
            })
        .collect(Collectors.toList());
  }
}
