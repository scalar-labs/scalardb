package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class DeleteStatementHandlerWithoutPrefixTest extends DeleteStatementHandlerTestBase {
  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.empty();
  }
}
