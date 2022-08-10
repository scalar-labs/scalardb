package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DynamoAdminTestWithDefaultConfigTest extends DynamoAdminTestBase {

  @Override
  Optional<String> getTableMetadataNamespaceConfig() {
    return Optional.empty();
  }

  @Override
  Optional<String> getNamespacePrefixConfig() {
    return Optional.empty();
  }
}
