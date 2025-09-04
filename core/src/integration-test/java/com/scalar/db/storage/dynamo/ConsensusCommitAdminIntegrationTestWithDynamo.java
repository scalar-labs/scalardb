package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitAdminIntegrationTestWithDynamo
    extends ConsensusCommitAdminIntegrationTestBase {

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
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
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
}
