package com.scalar.db.storage.hbase.query;

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

  /**
   * Enclose the target (schema, table or column) to use reserved words and special characters.
   *
   * @param name The target name to enclose
   * @return An enclosed string of the target name
   */
  public static String enclose(String name) {
    return "\"" + name + "\"";
  }

  public static String enclosedFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }
}
