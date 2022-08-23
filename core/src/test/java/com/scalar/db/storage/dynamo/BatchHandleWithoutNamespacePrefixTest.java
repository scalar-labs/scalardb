package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class BatchHandleWithoutNamespacePrefixTest extends BatchHandlerTestBase {
  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.empty();
  }
}
