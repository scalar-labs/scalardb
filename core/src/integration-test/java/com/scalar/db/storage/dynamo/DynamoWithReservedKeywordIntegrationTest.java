package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageWithReservedKeywordIntegrationTestBase;
import java.util.Map;

public class DynamoWithReservedKeywordIntegrationTest
    extends StorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected String getNamespace() {
    // a reserved keyword in DynamoDB
    return "space";
  }

  @Override
  protected String getTableName() {
    // a reserved keyword in DynamoDB
    return "table";
  }

  @Override
  protected String getColumnName1() {
    // a reserved keyword in DynamoDB
    return "from";
  }

  @Override
  protected String getColumnName2() {
    // a reserved keyword in DynamoDB
    return "to";
  }

  @Override
  protected String getColumnName3() {
    // a reserved keyword in DynamoDB
    return "values";
  }

  @Override
  protected String getColumnName4() {
    // a reserved keyword in DynamoDB
    return "like";
  }

  @Override
  protected String getColumnName5() {
    // a reserved keyword in DynamoDB
    return "status";
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
