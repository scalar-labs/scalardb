package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Column;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class checks if a record satisfies the conditions of Put and Delete operations that mutate
 * the record.
 */
@ThreadSafe
public class MutationConditionsValidator {
  private final String transactionId;

  public MutationConditionsValidator(String transactionId) {
    this.transactionId = transactionId;
  }

  /**
   * This checks if the condition of the specified Put operation is satisfied for the specified
   * record.
   *
   * @param put a Put operation
   * @param existingRecord the current value of the record targeted by the mutation, if any
   * @throws UnsatisfiedConditionException if the condition is not satisfied
   */
  public void checkIfConditionIsSatisfied(Put put, @Nullable TransactionResult existingRecord)
      throws UnsatisfiedConditionException {
    assert put.getCondition().isPresent();
    MutationCondition condition = put.getCondition().get();
    boolean recordExists = existingRecord != null;
    if (condition instanceof PutIf) {
      if (recordExists) {
        validateConditionalExpressions(condition.getExpressions(), existingRecord);
      } else {
        throwWhenRecordDoesNotExist(condition);
      }
    } else if (condition instanceof PutIfExists) {
      if (!recordExists) {
        throwWhenRecordDoesNotExist(condition);
      }
    } else if (condition instanceof PutIfNotExists) {
      if (recordExists) {
        throwWhenRecordExists(condition);
      }
    } else {
      throw new AssertionError();
    }
  }

  /**
   * This checks if the condition of the specified Delete operation is satisfied for the specified
   * record.
   *
   * @param delete a Delete operation
   * @param existingRecord the current value of the record targeted by the mutation, if any
   * @throws UnsatisfiedConditionException if the condition is not satisfied
   */
  public void checkIfConditionIsSatisfied(Delete delete, @Nullable TransactionResult existingRecord)
      throws UnsatisfiedConditionException {
    assert delete.getCondition().isPresent();
    MutationCondition condition = delete.getCondition().get();
    boolean recordExists = existingRecord != null;
    if (condition instanceof DeleteIf) {
      if (recordExists) {
        validateConditionalExpressions(condition.getExpressions(), existingRecord);
      } else {
        throwWhenRecordDoesNotExist(condition);
      }
    } else if (condition instanceof DeleteIfExists) {
      if (!recordExists) {
        throwWhenRecordDoesNotExist(condition);
      }
    } else {
      throw new AssertionError();
    }
  }

  private void throwWhenRecordDoesNotExist(MutationCondition condition)
      throws UnsatisfiedConditionException {
    throw new UnsatisfiedConditionException(
        CoreError.CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_NOT_EXISTS.buildMessage(
            condition.getClass().getSimpleName()),
        transactionId);
  }

  private void throwWhenRecordExists(MutationCondition condition)
      throws UnsatisfiedConditionException {
    throw new UnsatisfiedConditionException(
        CoreError.CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_EXISTS.buildMessage(
            condition.getClass().getSimpleName()),
        transactionId);
  }

  private void validateConditionalExpressions(
      List<ConditionalExpression> conditionalExpressions, TransactionResult existingRecord)
      throws UnsatisfiedConditionException {
    for (ConditionalExpression conditionalExpression : conditionalExpressions) {
      if (!shouldMutate(
          existingRecord.getColumns().get(conditionalExpression.getColumn().getName()),
          conditionalExpression.getColumn(),
          conditionalExpression.getOperator())) {
        throw new UnsatisfiedConditionException(
            CoreError.CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED.buildMessage(
                conditionalExpression.getColumn().getName()),
            transactionId);
      }
    }
  }

  private boolean shouldMutate(
      Column<?> existingRecordColumn, Column<?> conditionalExpressionColumn, Operator operator) {
    switch (operator) {
      case IS_NULL:
        return existingRecordColumn.hasNullValue();
      case IS_NOT_NULL:
        return !existingRecordColumn.hasNullValue();
      case EQ:
        return Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) == 0;
      case NE:
        return Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) != 0;
        // For 'greater than' and 'less than' types of conditions and when the existing record is
        // null, we consider the condition to be unsatisfied. This mimics the behavior as if
        // the condition was executed by the underlying storage
      case GT:
        return !existingRecordColumn.hasNullValue()
            && Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) > 0;
      case GTE:
        return !existingRecordColumn.hasNullValue()
            && Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) >= 0;
      case LT:
        return !existingRecordColumn.hasNullValue()
            && Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) < 0;
      case LTE:
        return !existingRecordColumn.hasNullValue()
            && Ordering.natural().compare(existingRecordColumn, conditionalExpressionColumn) <= 0;
      default:
        throw new AssertionError();
    }
  }
}
