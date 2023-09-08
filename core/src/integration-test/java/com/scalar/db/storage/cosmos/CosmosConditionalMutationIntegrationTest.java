package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class CosmosConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespace() {
    String namespace = super.getNamespace();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    return super.getOperatorAndDataTypeListForTest().stream()
        .filter(
            operatorAndDataType -> {
              // Cosmos DB only supports the 'equal' and 'not equal' and 'is null' and 'is not null'
              // conditions for BLOB type
              if (operatorAndDataType.getDataType() == DataType.BLOB) {
                return operatorAndDataType.getOperator() == Operator.EQ
                    || operatorAndDataType.getOperator() == Operator.NE
                    || operatorAndDataType.getOperator() == Operator.IS_NULL
                    || operatorAndDataType.getOperator() == Operator.IS_NOT_NULL;
              }
              return true;
            })
        .collect(Collectors.toList());
  }
}
