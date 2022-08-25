package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class SelectStatementHandlerWithPrefixTest extends SelectStatementHandlerTestBase {

  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.of("my_prefix_");
  }
}
