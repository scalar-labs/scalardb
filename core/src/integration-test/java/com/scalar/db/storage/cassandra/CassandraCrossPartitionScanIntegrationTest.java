package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cassandra")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
