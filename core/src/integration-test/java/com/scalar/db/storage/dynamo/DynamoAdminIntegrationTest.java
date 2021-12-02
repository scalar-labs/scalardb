package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

public class DynamoAdminIntegrationTest extends AdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  // Since DynamoDB doesn't have the namespace concept, some behaviors around the namespace are
  // different from the other adapters. So ignore several tests that check such behaviors

  @Ignore
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Ignore
  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldExecutionException() {}

  @Ignore
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldExecutionException() {}
}
