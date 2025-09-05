package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

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

  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly() {}

  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Disabled("DynamoDB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly() {}

  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_IfNotExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForPrimaryOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}
}
