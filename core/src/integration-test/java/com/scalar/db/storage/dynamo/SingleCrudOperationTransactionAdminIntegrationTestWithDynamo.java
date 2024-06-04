package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class SingleCrudOperationTransactionAdminIntegrationTestWithDynamo
    extends SingleCrudOperationTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  // Since DynamoDB doesn't have the namespace concept, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors

  @Disabled
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void namespaceExists_ShouldReturnCorrectResults() {}

  @Disabled
  @Test
  @Override
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}
}
