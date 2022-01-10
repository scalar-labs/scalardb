package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageWithReservedKeywordIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class CosmosWithReservedKeywordIntegrationTest
    extends StorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace() {
    String namespace = "reserved_keyword_test";
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
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
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
