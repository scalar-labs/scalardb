package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DynamoAdminWithNamespacePrefixAndTableMetadataNamespaceConfigTest
    extends DynamoAdminTestBase {
  @Override
  Optional<String> getTableMetadataNamespaceConfig() {
    return Optional.of("my_meta_ns");
  }

  @Override
  Optional<String> getNamespacePrefixConfig() {
    return Optional.of("my_prefix_");
  }
}
