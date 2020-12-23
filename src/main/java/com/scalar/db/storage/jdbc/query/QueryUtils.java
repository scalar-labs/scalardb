package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;

public final class QueryUtils {

  private QueryUtils() {}

  public static String getOperatorString(ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return "=";
      case NE:
        return "<>";
      case GT:
        return ">";
      case GTE:
        return ">=";
      case LT:
        return "<";
      case LTE:
        return "<=";
      default:
        throw new AssertionError("invalid operator: " + operator);
    }
  }
}
