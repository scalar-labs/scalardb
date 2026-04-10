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
  protected Stream<Arguments> provideLargeBlobSizes() {
    // Cosmos has a maximum item size of 2MB. Even though the BLOB data inserted is 1.5MB, when the
    // full record is serialized to JSON with UTF8 encoding, its approximate size is 2MB.
    return Stream.of(Arguments.of(1_500_000, "1.5 MB"));
  }
}
