package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraIntegrationTest extends DistributedStorageIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Disabled(
      "In Cassandra, if an IS NULL condition is specified for a column in a non-existing record, "
          + "the condition is considered satisfied. This behavior differs from that of other adapters")
  @Override
  @Test
  public void put_withPutIfIsNullWhenRecordDoesNotExist_shouldThrowNoMutationException() {}
}
