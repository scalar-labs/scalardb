package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosUtils.quoteKeyword;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import org.jooq.Field;
import org.jooq.SelectConditionStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

/**
 * A builder to make a query statement for a stored procedure of Cosmos DB from conditions
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConditionalQueryBuilder implements MutationConditionVisitor {
  private final SelectConditionStep<org.jooq.Record> select;
  private final ValueBinder binder;

  public ConditionalQueryBuilder(SelectConditionStep<org.jooq.Record> select) {
    this.select = select;
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
              e.getColumn().accept(binder);
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
              e.getColumn().accept(binder);
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
    Field<Object> field = DSL.field("r.values" + quoteKeyword(e.getColumn().getName()));
    switch (e.getOperator()) {
      case EQ:
        return v -> select.and(field.equal(v));
      case NE:
        return v -> select.and(field.notEqual(v));
      case GT:
        return v -> select.and(field.greaterThan(v));
      case GTE:
        return v -> select.and(field.greaterOrEqual(v));
      case LT:
        return v -> select.and(field.lessThan(v));
      case LTE:
        return v -> select.and(field.lessOrEqual(v));
      case IS_NULL:
        return v -> select.and("NOT IS_DEFINED(" + field.getName() + ")");
      case IS_NOT_NULL:
        return v -> select.and("IS_DEFINED(" + field.getName() + ")");
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
