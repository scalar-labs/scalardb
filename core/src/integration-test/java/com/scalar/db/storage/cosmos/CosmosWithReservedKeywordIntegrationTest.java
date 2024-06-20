package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageWithReservedKeywordIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class CosmosWithReservedKeywordIntegrationTest
    extends DistributedStorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespace() {
    return "reserved_keyword_test";
  }

  @Override
  protected String getTableName() {
    // a reserved keyword in Cosmos DB
    return "distinct";
  }

  @Override
  protected String getColumnName1() {
    // a reserved keyword in Cosmos DB
    return "from";
  }

  @Override
  protected String getColumnName2() {
    // a reserved keyword in Cosmos DB
    return "to";
  }

  @Override
  protected String getColumnName3() {
    // a reserved keyword in Cosmos DB
    return "value";
  }

  @Override
  protected String getColumnName4() {
    // a reserved keyword in Cosmos DB
    return "like";
  }

  @Override
  protected String getColumnName5() {
    // a reserved keyword in Cosmos DB
    return "top";
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
