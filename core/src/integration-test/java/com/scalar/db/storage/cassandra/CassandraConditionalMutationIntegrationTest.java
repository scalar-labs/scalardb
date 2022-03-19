package com.scalar.db.storage.cassandra;

import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.storage.StorageConditionalMutationIntegrationTestBase;

public class CassandraConditionalMutationIntegrationTest
    extends StorageConditionalMutationIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
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
