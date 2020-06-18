package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectSelectStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a query statement for a stored procedure of Cosmos DB from conditions
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConditionQueryBuilder implements MutationConditionVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConditionQueryBuilder.class);
  private final SelectSelectStep select;
  private final ValueBinder binder;

  public ConditionQueryBuilder(String id) {
    select =
        (SelectSelectStep)
            DSL.using(SQLDialect.DEFAULT).selectFrom("Record r").where(DSL.field("r.id").eq(id));
    binder = new ValueBinder();
  }

  public String getQuery() {
    return select.getSQL(ParamType.INLINED);
  }

  /**
   * Adds {@code PutIf}-specific conditions to the query
   *
   * @param condition {@code PutIf} condition
   */
  @Override
  public void visit(PutIf condition) {
    condition
        .getExpressions()
        .forEach(
            e -> {
              binder.set(createConditionWith(e));
              e.getValue().accept(binder);
            });
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
    condition
        .getExpressions()
        .forEach(
            e -> {
              binder.set(createConditionWith(e));
              e.getValue().accept(binder);
            });
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

  private <T> Consumer<T> createConditionWith(ConditionalExpression e) {
    // TODO: for a clustering key?
    Field field = DSL.field("r.values." + e.getName());
    switch (e.getOperator()) {
      case EQ:
        return v -> select.where(field.equal(v));
      case NE:
        return v -> select.where(field.notEqual(v));
      case GT:
        return v -> select.where(field.greaterThan(v));
      case GTE:
        return v -> select.where(field.greaterOrEqual(v));
      case LT:
        return v -> select.where(field.lessThan(v));
      case LTE:
        return v -> select.where(field.lessOrEqual(v));
      default:
        // never comes here because ConditionalExpression accepts only above operators
        throw new IllegalArgumentException(e.getOperator() + " is not supported");
    }
  }
}
