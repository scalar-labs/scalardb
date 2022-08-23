package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class SelectStatementHandlerWithoutPrefixTest extends SelectStatementHandlerTestBase {

  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.empty();
  }
}
