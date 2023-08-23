package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DynamoAdminWithNamespacePrefixConfigTest extends DynamoAdminTestBase {

  @Override
  Optional<String> getMetadataNamespaceConfig() {
    return Optional.empty();
  }

  @Override
  Optional<String> getNamespacePrefixConfig() {
    return Optional.of("my_prefix_");
  }
}
