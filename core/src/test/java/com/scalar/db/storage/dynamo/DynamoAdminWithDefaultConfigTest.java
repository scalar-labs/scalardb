package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DynamoAdminWithDefaultConfigTest extends DynamoAdminTestBase {

  @Override
  Optional<String> getMetadataNamespaceConfig() {
    return Optional.empty();
  }

  @Override
  Optional<String> getNamespacePrefixConfig() {
    return Optional.empty();
  }
}
