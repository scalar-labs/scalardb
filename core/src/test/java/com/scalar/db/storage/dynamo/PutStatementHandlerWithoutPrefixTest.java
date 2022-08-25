package com.scalar.db.storage.dynamo;

import java.util.Optional;

public class PutStatementHandlerWithoutPrefixTest extends PutStatementHandlerTestBase {
  @Override
  Optional<String> getNamespacePrefix() {
    return Optional.empty();
  }
}
