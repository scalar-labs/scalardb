package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class BatchHandleWithNamespacePrefixTest extends BatchHandlerTestBase {
  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.of("my_prefix_");
  }
}
