package com.scalar.db.storage.cassandra;

import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.Column;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
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

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
