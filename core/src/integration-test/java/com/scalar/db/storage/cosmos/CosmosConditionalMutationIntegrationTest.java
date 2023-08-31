package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

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
    List<OperatorAndDataType> ret = new ArrayList<>();
    for (Operator operator : Operator.values()) {
      if (operator == Operator.LIKE || operator == Operator.NOT_LIKE) {
        continue;
      }
      for (DataType dataType : DataType.values()) {
        // Cosmos DB only supports the 'equal' and 'not equal' and 'is null' and 'is not null'
        // conditions for BLOB type
        if (dataType == DataType.BLOB) {
          if (operator == Operator.EQ
              || operator == Operator.NE
              || operator == Operator.IS_NULL
              || operator == Operator.IS_NOT_NULL) {
            ret.add(new OperatorAndDataType(operator, dataType));
          }
        } else {
          ret.add(new OperatorAndDataType(operator, dataType));
        }
      }
    }
    return ret;
  }
}
