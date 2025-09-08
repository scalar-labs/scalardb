package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class SingleCrudOperationTransactionAdminIntegrationTestWithCassandra
    extends SingleCrudOperationTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }

  @Override
  @Disabled("Renaming non-primary key columns is not supported in Cassandra")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Renaming non-primary key columns is not supported in Cassandra")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly() {}
}
