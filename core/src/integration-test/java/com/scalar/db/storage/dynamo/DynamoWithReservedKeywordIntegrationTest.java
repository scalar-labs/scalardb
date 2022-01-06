package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageWithReservedKeywordIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class DynamoWithReservedKeywordIntegrationTest
    extends StorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected void initialize() {
    // reserved keywords in Dynamo
    NAMESPACE = "space";
    TABLE = "table";
    COL_NAME1 = "from";
    COL_NAME2 = "to";
    COL_NAME3 = "values";
    COL_NAME4 = "like";
    COL_NAME5 = "status";
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
