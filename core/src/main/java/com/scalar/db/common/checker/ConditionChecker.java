package com.scalar.db.common.checker;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.UpdateIf;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/** A checker for the conditions of mutations for the storage abstraction. */
@NotThreadSafe
public class ConditionChecker implements MutationConditionVisitor {
  private final TableMetadata tableMetadata;
  private boolean isPut;
  private boolean isValid;

  public ConditionChecker(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public boolean check(MutationCondition condition, boolean isPut) {
    this.isPut = isPut;
    condition.accept(this);
    return isValid;
  }

  @Override
  public void visit(PutIf condition) {
    if (!isPut) {
      isValid = false;
      return;
    }

    checkExpressions(condition.getExpressions());
  }

  @Override
  public void visit(PutIfExists condition) {
    isValid = isPut;
  }

  @Override
  public void visit(PutIfNotExists condition) {
    isValid = isPut;
  }

  @Override
  public void visit(DeleteIf condition) {
    if (isPut) {
      isValid = false;
      return;
    }

    checkExpressions(condition.getExpressions());
  }

  @Override
  public void visit(DeleteIfExists condition) {
    isValid = !isPut;
  }

  private void checkExpressions(List<ConditionalExpression> expressions) {
    for (ConditionalExpression expression : expressions) {
      if (expression.getOperator() == Operator.IS_NULL
          || expression.getOperator() == Operator.IS_NOT_NULL) {
        // the value must be null if the operator is 'is null' or `is not null`
        isValid =
            new ColumnChecker(tableMetadata, false, true, false, true)
                .check(expression.getColumn());
      } else {
        // Otherwise, the value must not be null
        isValid =
            new ColumnChecker(tableMetadata, true, false, false, true)
                .check(expression.getColumn());
      }
      if (!isValid) {
        break;
      }
    }
  }

  @Override
  public void visit(UpdateIf condition) {
    isValid = false;
  }
}
