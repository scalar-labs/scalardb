package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTransactionSessionIntegrationTestBase;
import java.util.Map;

public class SqlStatementWithTransactionSessionIntegrationTestWithDynamo
    extends SqlStatementWithTransactionSessionIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
