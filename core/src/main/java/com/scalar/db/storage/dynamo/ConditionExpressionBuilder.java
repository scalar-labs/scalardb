package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A builder to make a query statement for a stored procedure of Cosmos DB from conditions
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConditionExpressionBuilder implements MutationConditionVisitor {
  private final List<String> expressions;
  private final String columnNameAlias;
  private final String valueAlias;
  private int index;

  public ConditionExpressionBuilder(String columnNameAlias, String valueAlias) {
    this.expressions = new ArrayList<>();
    this.columnNameAlias = columnNameAlias;
    this.valueAlias = valueAlias;
    this.index = 0;
  }

  @Nonnull
  public String build() {
    return String.join(" AND ", expressions);
  }

  /**
   * Adds {@code PutIf}-specific conditions to the query
   *
   * @param condition {@code PutIf} condition
   */
  @Override
  public void visit(PutIf condition) {
    condition.getExpressions().forEach(e -> expressions.add(createConditionWith(e)));
  }

  /**
   * Adds {@code PutIfExists}-specific conditions to the query
   *
   * @param condition {@code PutIfExists} condition
   */
  @Override
  public void visit(PutIfExists condition) {
    // nothing to do
  }

  /**
   * Adds {@code PutIfNotExists}-specific conditions to the query
   *
   * @param condition {@code PutIfNotExists} condition
   */
  @Override
  public void visit(PutIfNotExists condition) {
    // nothing to do
  }

  /**
   * Adds {@code DeleteIf}-specific conditions to the query
   *
   * @param condition {@code DeleteIf} condition
   */
  @Override
  public void visit(DeleteIf condition) {
    condition.getExpressions().forEach(e -> expressions.add(createConditionWith(e)));
  }

  /**
   * Adds {@code DeleteIfExists}-specific conditions to the query
   *
   * @param condition {@code DeleteIfExists} condition
   */
  @Override
  public void visit(DeleteIfExists condition) {
    // nothing to do
  }

  private String createConditionWith(ConditionalExpression e) {
    List<String> elements;

    String columnName = columnNameAlias + index;
    String value = valueAlias + index;
    switch (e.getOperator()) {
      case EQ:
        elements = Arrays.asList(columnName, "=", value);
        break;
      case NE:
        elements = Arrays.asList("NOT", columnName, "=", value);
        break;
      case GT:
        elements = Arrays.asList(columnName, ">", value);
        break;
      case GTE:
        elements = Arrays.asList(columnName, ">=", value);
        break;
      case LT:
        elements = Arrays.asList(columnName, "<", value);
        break;
      case LTE:
        elements = Arrays.asList(columnName, "<=", value);
        break;
      case IS_NULL:
        elements =
            Arrays.asList(
                "(attribute_not_exists(" + columnName + ")", "OR", columnName, "=", value + ")");
        break;
      case IS_NOT_NULL:
        elements =
            Arrays.asList(
                "(attribute_exists(" + columnName + ")",
                "AND",
                "NOT",
                columnName,
                "=",
                value + ")");
        break;
      default:
        throw new AssertionError();
    }
    index++;

    return String.join(" ", elements);
  }

  @Override
  public void visit(UpdateIf condition) {
    throw new AssertionError("UpdateIf is not supported");
  }

  @Override
  public void visit(UpdateIfExists condition) {
    throw new AssertionError("UpdateIfExists is not supported");
  }
}
