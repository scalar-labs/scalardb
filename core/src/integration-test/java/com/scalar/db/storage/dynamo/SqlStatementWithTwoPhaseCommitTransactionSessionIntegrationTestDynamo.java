package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase;
import java.util.Map;

public class SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestDynamo
    extends SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
