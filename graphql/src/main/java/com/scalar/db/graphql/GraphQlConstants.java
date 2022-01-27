package com.scalar.db.graphql;

import com.google.common.collect.ImmutableSet;

public final class GraphQlConstants {
  public static final String CONTEXT_TRANSACTION_KEY = "transaction";
  public static final ImmutableSet<String> SCALAR_VALUE_FIELD_NAMES =
      ImmutableSet.of(
          "intValue", "bigIntValue", "floatValue", "doubleValue", "textValue", "booleanValue");

  private GraphQlConstants() {}
}
