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
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class checks if a record satisfies the conditions of Put and Delete operations that mutate
 * the record.
 */
@ThreadSafe
public class MutationConditionsValidator {

  private final Optional<CollationComparator> collationComparator;

  /** Creates a validator that keeps natural-order, byte-exact behavior on TEXT. */
  public MutationConditionsValidator() {
    this(Optional.empty());
  }

  /**
   * Creates a validator that uses the given collation for range operators (GT/GTE/LT/LTE) on TEXT
   * columns, and for EQ/NE on TEXT when the comparator has nondeterministic equality enabled.
   * Otherwise EQ/NE stay byte-exact. Identity operators (IS_NULL/IS_NOT_NULL) are always unchanged.
   *
   * @param collationComparator the collation comparator, or {@link Optional#empty()} to keep
   *     natural-order behavior
   */
  public MutationConditionsValidator(Optional<CollationComparator> collationComparator) {
    this.collationComparator = collationComparator;
  }

  /**
   * This checks if the condition of the specified Put operation is satisfied for the specified
   * record.
   *
   * @param put a Put operation
   * @param existingRecord the current value of the record targeted by the mutation, if any
   * @param transactionId the transaction ID
   * @throws UnsatisfiedConditionException if the condition is not satisfied
   */
  public void checkIfConditionIsSatisfied(
      Put put, @Nullable TransactionResult existingRecord, String transactionId)
      throws UnsatisfiedConditionException {
    assert put.getCondition().isPresent();
    MutationCondition condition = put.getCondition().get();
    boolean recordExists = existingRecord != null;
    if (condition instanceof PutIf) {
      if (recordExists) {
        validateConditionalExpressions(condition.getExpressions(), existingRecord, transactionId);
      } else {
        throwWhenRecordDoesNotExist(condition, transactionId);
      }
    } else if (condition instanceof PutIfExists) {
      if (!recordExists) {
        throwWhenRecordDoesNotExist(condition, transactionId);
      }
    } else if (condition instanceof PutIfNotExists) {
      if (recordExists) {
        throwWhenRecordExists(condition, transactionId);
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
   * @param transactionId the transaction ID
   * @throws UnsatisfiedConditionException if the condition is not satisfied
   */
  public void checkIfConditionIsSatisfied(
      Delete delete, @Nullable TransactionResult existingRecord, String transactionId)
      throws UnsatisfiedConditionException {
    assert delete.getCondition().isPresent();
    MutationCondition condition = delete.getCondition().get();
    boolean recordExists = existingRecord != null;
    if (condition instanceof DeleteIf) {
      if (recordExists) {
        validateConditionalExpressions(condition.getExpressions(), existingRecord, transactionId);
      } else {
        throwWhenRecordDoesNotExist(condition, transactionId);
      }
    } else if (condition instanceof DeleteIfExists) {
      if (!recordExists) {
        throwWhenRecordDoesNotExist(condition, transactionId);
      }
    } else {
      throw new AssertionError();
    }
  }

  private void throwWhenRecordDoesNotExist(MutationCondition condition, String transactionId)
      throws UnsatisfiedConditionException {
    throw new UnsatisfiedConditionException(
        CoreError.CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_NOT_EXISTS.buildMessage(
            condition.getClass().getSimpleName()),
        transactionId);
  }

  private void throwWhenRecordExists(MutationCondition condition, String transactionId)
      throws UnsatisfiedConditionException {
    throw new UnsatisfiedConditionException(
        CoreError.CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_EXISTS.buildMessage(
            condition.getClass().getSimpleName()),
        transactionId);
  }

  private void validateConditionalExpressions(
      List<ConditionalExpression> conditionalExpressions,
      TransactionResult existingRecord,
      String transactionId)
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
        return equalsForCondition(existingRecordColumn, conditionalExpressionColumn);
      case NE:
        return !equalsForCondition(existingRecordColumn, conditionalExpressionColumn);
        // For 'greater than' and 'less than' types of conditions and when the existing record is
        // null, we consider the condition to be unsatisfied. This mimics the behavior as if
        // the condition was executed by the underlying storage
      case GT:
        return !existingRecordColumn.hasNullValue()
            && rangeCompare(existingRecordColumn, conditionalExpressionColumn) > 0;
      case GTE:
        return !existingRecordColumn.hasNullValue()
            && rangeCompare(existingRecordColumn, conditionalExpressionColumn) >= 0;
      case LT:
        return !existingRecordColumn.hasNullValue()
            && rangeCompare(existingRecordColumn, conditionalExpressionColumn) < 0;
      case LTE:
        return !existingRecordColumn.hasNullValue()
            && rangeCompare(existingRecordColumn, conditionalExpressionColumn) <= 0;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Decides {@code EQ} (and, negated, {@code NE}) for a conditional mutation. When a collation is
   * present with nondeterministic equality and the existing column is {@code TEXT}, equality is
   * decided by the collation (both text values non-null); otherwise it stays byte-exact via natural
   * ordering.
   */
  private boolean equalsForCondition(Column<?> existing, Column<?> conditionValue) {
    if (collationComparator.isPresent()
        && collationComparator.get().isNondeterministicEquality()
        && existing.getDataType() == DataType.TEXT) {
      String a = existing.getTextValue();
      String b = conditionValue.getTextValue();
      if (a != null && b != null) {
        return collationComparator.get().textEquals(a, b);
      }
    }
    return Ordering.natural().compare(existing, conditionValue) == 0;
  }

  private int rangeCompare(Column<?> a, Column<?> b) {
    Comparator<Column<?>> rangeComparator =
        collationComparator
            .map(CollationComparator::columnComparator)
            .orElseGet(() -> (x, y) -> Ordering.natural().compare(x, y));
    return rangeComparator.compare(a, b);
  }
}
