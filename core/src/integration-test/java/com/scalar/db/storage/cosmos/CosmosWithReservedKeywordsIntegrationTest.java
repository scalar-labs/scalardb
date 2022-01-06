package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageWithReservedKeywordIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Optional;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class CosmosWithReservedKeywordsIntegrationTest
    extends StorageWithReservedKeywordIntegrationTestBase {
  @Override
  protected void initialize() {
    // reserved keywords in Cosmos
    NAMESPACE = "between";
    TABLE = "distinct";
    COL_NAME1 = "from";
    COL_NAME2 = "to";
    COL_NAME3 = "value";
    COL_NAME4 = "like";
    COL_NAME5 = "top";
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace() {
    String namespace = super.getNamespace();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
