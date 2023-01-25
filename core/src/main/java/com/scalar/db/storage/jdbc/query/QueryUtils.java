package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;

public final class QueryUtils {

  private QueryUtils() {}

  public static String getConditionString(
      String columnName, ConditionalExpression.Operator operator, RdbEngineStrategy rdbEngine) {
    switch (operator) {
      case EQ:
        return rdbEngine.enclose(columnName) + "=?";
      case NE:
        return rdbEngine.enclose(columnName) + "<>?";
      case GT:
        return rdbEngine.enclose(columnName) + ">?";
      case GTE:
        return rdbEngine.enclose(columnName) + ">=?";
      case LT:
        return rdbEngine.enclose(columnName) + "<?";
      case LTE:
        return rdbEngine.enclose(columnName) + "<=?";
      case IS_NULL:
        return rdbEngine.enclose(columnName) + " IS NULL";
      case IS_NOT_NULL:
        return rdbEngine.enclose(columnName) + " IS NOT NULL";
      default:
        throw new AssertionError();
    }
  }
}
