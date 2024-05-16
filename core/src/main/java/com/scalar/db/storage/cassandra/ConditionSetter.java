package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ne;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  @SuppressFBWarnings("EI_EXPOSE_REP2")
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
        .forEach(i -> cond.and(createClauseWith(expressions.get(i))));
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
        .forEach(i -> cond.and(createClauseWith(expressions.get(i))));
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
    String name = quoteIfNecessary(e.getName());
    switch (e.getOperator()) {
      case EQ:
      case IS_NULL:
        return eq(name, bindMarker());
      case NE:
      case IS_NOT_NULL:
        return ne(name, bindMarker());
      case GT:
        return gt(name, bindMarker());
      case GTE:
        return gte(name, bindMarker());
      case LT:
        return lt(name, bindMarker());
      case LTE:
        return lte(name, bindMarker());
      default:
        throw new AssertionError();
    }
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
