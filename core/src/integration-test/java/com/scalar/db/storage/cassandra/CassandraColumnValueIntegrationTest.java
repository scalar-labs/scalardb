package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class CassandraColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
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

  protected Stream<Arguments> provideLargeBlobSizes() {
    return Stream.of(
        // Cassandra has a default maximum mutation size is 16MB
        Arguments.of(16_000_000, "16 MB"));
  }
}
