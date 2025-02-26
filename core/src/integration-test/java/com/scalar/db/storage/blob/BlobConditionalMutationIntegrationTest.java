package com.scalar.db.storage.blob;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class BlobConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    return super.getOperatorAndDataTypeListForTest().stream()
        .filter(
            operatorAndDataType ->
                operatorAndDataType.getOperator() == ConditionalExpression.Operator.EQ
                    || operatorAndDataType.getOperator() == ConditionalExpression.Operator.NE)
        .collect(Collectors.toList());
  }
}
