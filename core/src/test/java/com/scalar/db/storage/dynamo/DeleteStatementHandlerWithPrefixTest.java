package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DeleteStatementHandlerWithPrefixTest extends DeleteStatementHandlerTestBase {
  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.of("my_prefix_");
  }
}
