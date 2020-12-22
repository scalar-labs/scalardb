package com.scalar.db.storage.jdbc.checker;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ConditionChecker implements MutationConditionVisitor {
  private final JdbcTableMetadata tableMetadata;
  private boolean isPut;
  private boolean okay;

  public ConditionChecker(JdbcTableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public boolean check(MutationCondition condition, boolean isPut) {
    this.isPut = isPut;
    condition.accept(this);
    return okay;
  }

  @Override
  public void visit(PutIf condition) {
    if (!isPut) {
      okay = false;
      return;
    }

    // Check the values in the expressions
    for (ConditionalExpression expression : condition.getExpressions()) {
      okay =
          new ColumnDataTypeChecker(tableMetadata)
              .check(expression.getName(), expression.getValue());
      if (!okay) {
        break;
      }
    }
  }

  @Override
  public void visit(PutIfExists condition) {
    okay = isPut;
  }

  @Override
  public void visit(PutIfNotExists condition) {
    okay = isPut;
  }

  @Override
  public void visit(DeleteIf condition) {
    if (isPut) {
      okay = false;
      return;
    }

    // Check the values in the expressions
    for (ConditionalExpression expression : condition.getExpressions()) {
      okay =
          new ColumnDataTypeChecker(tableMetadata)
              .check(expression.getName(), expression.getValue());
      if (!okay) {
        break;
      }
    }
  }

  @Override
  public void visit(DeleteIfExists condition) {
    okay = !isPut;
  }
}
