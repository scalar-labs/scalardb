package com.scalar.database.storage.cassandra4driver;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.DeleteIf;
import com.scalar.database.api.DeleteIfExists;
import com.scalar.database.api.MutationConditionVisitor;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.PutIfExists;
import com.scalar.database.api.PutIfNotExists;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to configure condition-specific statement
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@NotThreadSafe
public class ConditionSetter implements MutationConditionVisitor {
  private BuildableQuery query;

  /**
   * Constructs {@code ConditionSetter} with the specified {@code BuildableQuery}
   *
   * @param query {@code BuildableQuery} to set conditions
   */
  public ConditionSetter(BuildableQuery query) {
    this.query = query;
  }

  public BuildableQuery getQuery() {
    return query;
  }

  /**
   * Adds {@code PutIf}-specific conditions to the statement
   *
   * @param condition {@code PutIf} condition
   */
  @Override
  public void visit(PutIf condition) {
    List<ConditionalExpression> expressions = condition.getExpressions();
    List<Condition> conditions = new ArrayList<>();
    IntStream.range(0, expressions.size())
        .forEach(
            i -> {
              conditions.add(createConditionWith(expressions.get(i)));
            });

    query = ((Update) query).if_(conditions);
  }

  /**
   * Adds {@code PutIfExists}-specific conditions to the statement
   *
   * @param condition {@code PutIfExists} condition
   */
  @Override
  public void visit(PutIfExists condition) {
    query = ((Update) query).ifExists();
  }

  /**
   * Adds {@code PutIfNotExists}-specific conditions to the statement
   *
   * @param condition {@code PutIfNotExists} condition
   */
  @Override
  public void visit(PutIfNotExists condition) {
    query = ((Insert) query).ifNotExists();
  }

  /**
   * Adds {@code DeleteIf}-specific conditions to the statement
   *
   * @param condition {@code DeleteIf} condition
   */
  @Override
  public void visit(DeleteIf condition) {
    List<ConditionalExpression> expressions = condition.getExpressions();
    List<Condition> conditions = new ArrayList<>();
    IntStream.range(0, expressions.size())
        .forEach(
            i -> {
              conditions.add(createConditionWith(expressions.get(i)));
            });

    query = ((Delete) query).if_(conditions);
  }

  /**
   * Adds {@code DeleteIfExists}-specific conditions to the statement
   *
   * @param condition {@code DeleteIfExists} condition
   */
  @Override
  public void visit(DeleteIfExists condition) {
    query = ((Delete) query).ifExists();
  }

  private Condition createConditionWith(ConditionalExpression e) {
    switch (e.getOperator()) {
      case EQ:
        return Condition.column(e.getName()).isEqualTo(bindMarker());
      case NE:
        return Condition.column(e.getName()).isNotEqualTo(bindMarker());
      case GT:
        return Condition.column(e.getName()).isGreaterThan(bindMarker());
      case GTE:
        return Condition.column(e.getName()).isGreaterThanOrEqualTo(bindMarker());
      case LT:
        return Condition.column(e.getName()).isLessThan(bindMarker());
      case LTE:
        return Condition.column(e.getName()).isLessThanOrEqualTo(bindMarker());
      default:
        // never comes here because ConditionalExpression accepts only above operators
        throw new IllegalArgumentException(e.getOperator() + " is not supported");
    }
  }
}
