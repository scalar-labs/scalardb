package com.scalar.db.graphql.schema;

import com.google.common.collect.ImmutableSet;

public final class Constants {
  public static final String TRANSACTION_DIRECTIVE_NAME = "transaction";
  public static final String TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME = "txId";
  public static final String TRANSACTION_DIRECTIVE_COMMIT_ARGUMENT_NAME = "commit";
  public static final String CONTEXT_TRANSACTION_KEY = "transaction";
  public static final String ERRORS_EXTENSIONS_EXCEPTION_KEY = "exception";
  public static final ImmutableSet<String> SCALAR_VALUE_KEYS =
      ImmutableSet.of(
          "intValue", "bigIntValue", "floatValue", "doubleValue", "textValue", "booleanValue");

  private Constants() {}
}
