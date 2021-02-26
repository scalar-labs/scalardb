package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.storage.jdbc.RdbEngine;

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
   * @param rdbEngine The RDB engine. The enclosing character is different depending on the RDB
   *     engine
   * @return An enclosed string of the target name
   */
  public static String enclose(String name, RdbEngine rdbEngine) {
    switch (rdbEngine) {
      case MYSQL:
        return "`" + name + "`";
      case POSTGRESQL:
      case ORACLE:
        return "\"" + name + "\"";
      case SQL_SERVER:
      default:
        return "[" + name + "]";
    }
  }

  public static String enclosedFullTableName(String schema, String table, RdbEngine rdbEngine) {
    return enclose(schema, rdbEngine) + "." + enclose(table, rdbEngine);
  }
}
