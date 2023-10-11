package com.scalar.db.storage.cassandra;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CassandraAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected void waitForCreationIfNecessary() {
    // In some of the tests, we modify metadata in one Cassandra cluster session (via the
    // CassandraAdmin) and verify if such metadata were updated by using another session (via the
    // CassandraAdminTestUtils). But it takes some time for metadata change to be propagated from
    // one session to the other, so we need to wait
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
  }
}
