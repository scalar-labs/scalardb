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
import com.scalar.db.exception.transaction.PreparationUnsatisfiedConditionException;
import com.scalar.db.io.Column;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class verifies the mutation condition for Put and Delete operation are met by asserting the
 * condition on the existing record (if any) targeted by the mutation. It does not leverage the
 * underlying storage to validate the condition.
 */
@ThreadSafe
public class ConditionalMutationValidator {
  private final String transactionId;

  public ConditionalMutationValidator(String transactionId) {
    this.transactionId = transactionId;
  }

  /**
   * This validates the condition for the Put operation is satisfied.
   *
   * @param put a Put operation
   * @param existingRecord the current value of the record targeted by the mutation, if any
   * @throws PreparationUnsatisfiedConditionException if the condition is not satisfied
   */
  public void validateConditionIsSatisfied(Put put, @Nullable TransactionResult existingRecord)
      throws PreparationUnsatisfiedConditionException {
    if (!put.getCondition().isPresent()) {
      return;
    }
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
   * This validates the condition for the Delete operation is satisfied.
   *
   * @param delete a Delete operation
   * @param existingRecord the current value of the record targeted by the mutation, if any
   * @throws PreparationUnsatisfiedConditionException if the condition is not satisfied
   */
  public void validateConditionIsSatisfied(
      Delete delete, @Nullable TransactionResult existingRecord)
      throws PreparationUnsatisfiedConditionException {
    if (!delete.getCondition().isPresent()) {
      return;
    }
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
      throws PreparationUnsatisfiedConditionException {
    throw new PreparationUnsatisfiedConditionException(
        "The record does not exist so the "
            + condition.getClass().getSimpleName()
            + " condition is not satisfied",
        transactionId);
  }

  private void throwWhenRecordExists(MutationCondition condition)
      throws PreparationUnsatisfiedConditionException {
    throw new PreparationUnsatisfiedConditionException(
        "The record exists so the "
            + condition.getClass().getSimpleName()
            + " condition is not satisfied",
        transactionId);
  }

  private void validateConditionalExpressions(
      List<ConditionalExpression> conditionalExpressions, TransactionResult existingRecord)
      throws PreparationUnsatisfiedConditionException {
    for (ConditionalExpression conditionalExpression : conditionalExpressions) {
      if (!shouldMutate(
          existingRecord.getColumns().get(conditionalExpression.getColumn().getName()),
          conditionalExpression.getColumn(),
          conditionalExpression.getOperator())) {
        throw new PreparationUnsatisfiedConditionException(
            "The condition on the column '"
                + conditionalExpression.getColumn().getName()
                + "' is not satisfied",
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
