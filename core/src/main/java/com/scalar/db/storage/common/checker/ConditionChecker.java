package com.scalar.db.storage.common.checker;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ConditionChecker implements MutationConditionVisitor {
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

    // Check the values in the expressions
    for (ConditionalExpression expression : condition.getExpressions()) {
      isValid =
          new ColumnChecker(tableMetadata, false, false, true)
              .check(expression.getName(), expression.getValue());
      if (!isValid) {
        break;
      }
    }
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

    // Check the values in the expressions
    for (ConditionalExpression expression : condition.getExpressions()) {
      isValid =
          new ColumnChecker(tableMetadata, false, false, true)
              .check(expression.getName(), expression.getValue());
      if (!isValid) {
        break;
      }
    }
  }

  @Override
  public void visit(DeleteIfExists condition) {
    isValid = !isPut;
  }
}
