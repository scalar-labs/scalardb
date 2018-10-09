package com.scalar.database.storage.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.DeleteIf;
import com.scalar.database.api.DeleteIfExists;
import com.scalar.database.api.MutationConditionVisitor;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.PutIfExists;
import com.scalar.database.api.PutIfNotExists;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to configure condition-specific statement
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class ConditionSetter implements MutationConditionVisitor {
  private final BuiltStatement statement;

  /**
   * Constructs {@code ConditionSetter} with the specified {@code BuiltStatement}
   *
   * @param statement {@code BuiltStatement} to set conditions
   */
  public ConditionSetter(BuiltStatement statement) {
    this.statement = statement;
  }

  /**
   * Adds {@code PutIf}-specific conditions to the statement
   *
   * @param condition {@code PutIf} condition
   */
  @Override
  public void visit(PutIf condition) {
    Update.Where update = (Update.Where) statement;

    List<ConditionalExpression> expressions = condition.getExpressions();
    Update.Conditions cond = update.onlyIf(createClauseWith(expressions.get(0)));
    IntStream.range(1, expressions.size())
        .forEach(
            i -> {
              cond.and(createClauseWith(expressions.get(i)));
            });
  }

  /**
   * Adds {@code PutIfExists}-specific conditions to the statement
   *
   * @param condition {@code PutIfExists} condition
   */
  @Override
  public void visit(PutIfExists condition) {
    Update.Where update = (Update.Where) statement;
    update.ifExists();
  }

  /**
   * Adds {@code PutIfNotExists}-specific conditions to the statement
   *
   * @param condition {@code PutIfNotExists} condition
   */
  @Override
  public void visit(PutIfNotExists condition) {
    Insert insert = (Insert) statement;
    insert.ifNotExists();
  }

  /**
   * Adds {@code DeleteIf}-specific conditions to the statement
   *
   * @param condition {@code DeleteIf} condition
   */
  @Override
  public void visit(DeleteIf condition) {
    Delete.Where delete = (Delete.Where) statement;

    List<ConditionalExpression> expressions = condition.getExpressions();
    Delete.Conditions cond = delete.onlyIf(createClauseWith(expressions.get(0)));
    IntStream.range(1, expressions.size())
        .forEach(
            i -> {
              cond.and(createClauseWith(expressions.get(i)));
            });
  }

  /**
   * Adds {@code DeleteIfExists}-specific conditions to the statement
   *
   * @param condition {@code DeleteIfExists} condition
   */
  @Override
  public void visit(DeleteIfExists condition) {
    Delete.Where delete = (Delete.Where) statement;
    delete.ifExists();
  }

  private Clause createClauseWith(ConditionalExpression e) {
    switch (e.getOperator()) {
      case EQ:
        return eq(e.getName(), bindMarker());
      case NE:
        return ne(e.getName(), bindMarker());
      case GT:
        return gt(e.getName(), bindMarker());
      case GTE:
        return gte(e.getName(), bindMarker());
      case LT:
        return lt(e.getName(), bindMarker());
      case LTE:
        return lte(e.getName(), bindMarker());
      default:
        // never comes here because ConditionalExpression accepts only above operators
        throw new IllegalArgumentException(e.getOperator() + " is not supported");
    }
  }
}
