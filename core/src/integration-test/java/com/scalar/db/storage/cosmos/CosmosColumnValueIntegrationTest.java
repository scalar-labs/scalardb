package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class CosmosColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected Stream<Arguments> provideBlobSizes() {
    // Cosmos has a maximum item size of 2MB
    return Stream.of(
        Arguments.of(1_900_000, "1.9 MB"),
        Arguments.of(2_000_000, "2 MB"),
        Arguments.of(2_100_000, "2.1 MB"));
  }
}
