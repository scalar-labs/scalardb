package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageWithReservedKeywordIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoWithReservedKeywordIntegrationTest
    extends DistributedStorageWithReservedKeywordIntegrationTestBase {

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
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
