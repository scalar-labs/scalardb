package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class SingleCrudOperationTransactionAdminIntegrationTestWithCosmos
    extends SingleCrudOperationTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_IfNotExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForPrimaryOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}
}
