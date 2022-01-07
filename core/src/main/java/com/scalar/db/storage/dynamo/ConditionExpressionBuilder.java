package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final String alias;
  private int index;
  private final Map<String, String> conditionAttributeNameMap;
  static final String CONDITION_ATTRIBUTE_NAME_PREFIX = "#con_att_";

  public ConditionExpressionBuilder(String alias) {
    this.expressions = new ArrayList<>();
    this.alias = alias;
    this.index = 0;
    conditionAttributeNameMap = new HashMap<>();
  }

  @Nonnull
  public String build() {
    return String.join(" AND ", expressions);
  }

  public Map<String, String> getConditionAttributeNameMap() {
    return conditionAttributeNameMap;
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
    String elementName = CONDITION_ATTRIBUTE_NAME_PREFIX + e.getName();
    conditionAttributeNameMap.put(elementName, e.getName());
    switch (e.getOperator()) {
      case EQ:
        elements = Arrays.asList(elementName, "=", alias + index);
        break;
      case NE:
        elements = Arrays.asList("NOT", elementName, "=", alias + index);
        break;
      case GT:
        elements = Arrays.asList(elementName, ">", alias + index);
        break;
      case GTE:
        elements = Arrays.asList(elementName, ">=", alias + index);
        break;
      case LT:
        elements = Arrays.asList(elementName, "<", alias + index);
        break;
      case LTE:
        elements = Arrays.asList(elementName, "<=", alias + index);
        break;
      default:
        // never comes here because ConditionalExpression accepts only above operators
        throw new IllegalArgumentException(e.getOperator() + " is not supported");
    }
    index++;

    return String.join(" ", elements);
  }
}
