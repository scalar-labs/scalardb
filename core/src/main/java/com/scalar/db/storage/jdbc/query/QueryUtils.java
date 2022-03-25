package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.storage.jdbc.RdbEngine;

public final class QueryUtils {

  private QueryUtils() {}

  public static String getConditionString(
      String columnName, ConditionalExpression.Operator operator, RdbEngine rdbEngine) {
    switch (operator) {
      case EQ:
        return enclose(columnName, rdbEngine) + "=?";
      case NE:
        return enclose(columnName, rdbEngine) + "<>?";
      case GT:
        return enclose(columnName, rdbEngine) + ">?";
      case GTE:
        return enclose(columnName, rdbEngine) + ">=?";
      case LT:
        return enclose(columnName, rdbEngine) + "<?";
      case LTE:
        return enclose(columnName, rdbEngine) + "<=?";
      case IS_NULL:
        return enclose(columnName, rdbEngine) + " IS NULL";
      case IS_NOT_NULL:
        return enclose(columnName, rdbEngine) + " IS NOT NULL";
      default:
        throw new AssertionError();
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
