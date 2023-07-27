package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Column;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MutationConditionsValidatorTest {
  private MutationConditionsValidator validator;
  private @Mock TransactionResult existingRecord;
  private @Mock Put put;
  private @Mock Delete delete;
  private @Mock PutIf putIf;
  private @Mock PutIfExists putIfExists;
  private @Mock PutIfNotExists putIfNotExists;
  private @Mock DeleteIf deleteIf;
  private @Mock DeleteIfExists deleteIfExists;
  private static final String C1 = "col_1";
  private static final String C2 = "col_2";
  private static final String C3 = "col_3";

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    validator = new MutationConditionsValidator("a_tx_id");
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutIfConditionWhenRecordDoesNotExist_ShouldThrowUnsatisfiedConditionException() {
    // Arrange
    prepareMutationWithCondition(put, putIf);

    // Act Assert
    Assertions.assertThatThrownBy(() -> validator.checkIfConditionIsSatisfied(put, null))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutIfExistsConditionWhenRecordDoesNotExist_ShouldThrowUnsatisfiedConditionException() {
    // Arrange
    prepareMutationWithCondition(put, putIfExists);

    // Act Assert
    Assertions.assertThatThrownBy(() -> validator.checkIfConditionIsSatisfied(put, null))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutIfExistsConditionWhenRecordExists_ShouldNotThrow() {
    // Arrange
    prepareMutationWithCondition(put, putIfExists);

    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(put, existingRecord))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutIfNotExistsConditionWhenRecordExists_ShouldThrowUnsatisfiedConditionException() {
    // Arrange
    prepareMutationWithCondition(put, putIfNotExists);

    // Act Assert
    Assertions.assertThatThrownBy(() -> validator.checkIfConditionIsSatisfied(put, existingRecord))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutIfNotExistsConditionWhenRecordDoesNotExist_ShouldNotThrow() {
    // Arrange
    prepareMutationWithCondition(put, putIfNotExists);

    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(put, null))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithDeleteIfConditionWhenRecordDoesNotExist_ShouldThrowUnsatisfiedConditionException() {
    // Arrange
    prepareMutationWithCondition(delete, deleteIf);

    // Act Assert
    Assertions.assertThatThrownBy(() -> validator.checkIfConditionIsSatisfied(delete, null))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void
      validateConditionIsSatisfied_WithDeleteIfExistsConditionWhenRecordDoesNotExist_ShouldThrowUnsatisfiedConditionException() {
    // Arrange
    prepareMutationWithCondition(delete, deleteIfExists);

    // Act Assert
    Assertions.assertThatThrownBy(() -> validator.checkIfConditionIsSatisfied(delete, null))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void
      validateConditionIsSatisfied_WithDeleteIfExistsConditionWhenRecordExists_ShouldNotThrow() {
    // Arrange
    prepareMutationWithCondition(delete, deleteIfExists);

    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(delete, existingRecord))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutWithoutConditionWhenRecordExists_ShouldNotThrow() {
    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(put, existingRecord))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithPutWithoutConditionWhenRecordDoesNotExist_ShouldNotThrow() {
    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(put, null))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithDeleteWithoutConditionWhenRecordExists_ShouldNotThrow() {
    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(delete, existingRecord))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditionIsSatisfied_WithDeleteWithoutConditionWhenRecordDoesNotExist_ShouldNotThrow() {
    // Act Assert
    Assertions.assertThatCode(() -> validator.checkIfConditionIsSatisfied(delete, null))
        .doesNotThrowAnyException();
  }

  private void prepareMutationWithCondition(Mutation mutation, MutationCondition condition) {
    when(mutation.getCondition()).thenReturn(Optional.of(condition));
  }

  @Test
  public void
      validateConditionIsSatisfied_ForPutIfAndDeleteIfWithSingleConditionWhenRecordExists_ShouldThrowUnsatisfiedConditionExceptionIfConditionIsNotSatisfied() {
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.EQ, 0, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.EQ, -1, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.NE, 0, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.NE, -1, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GT, null, true, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GT, -1, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GT, 0, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GT, 1, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GTE, null, true, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GTE, -1, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GTE, 0, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.GTE, 1, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LT, null, true, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LT, -1, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LT, 0, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LT, 1, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LTE, null, true, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LTE, -1, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LTE, 0, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LTE, 1, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.LT, null, true, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.IS_NULL, null, false, false);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.IS_NULL, null, true, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.IS_NOT_NULL, null, false, true);
    assertWithSingleConditionForPutIfAndDeleteIf(Operator.IS_NOT_NULL, null, true, false);
  }

  @Test
  public void
      validateConditionIsSatisfied_ForPutIfAndDeleteIfWithSeveralConditionsWhenRecordExists_ShouldThrowUnsatisfiedConditionExceptionIfConditionIsNotValidated() {
    for (boolean isCondition1Satisfied : ImmutableList.of(true, false)) {
      for (boolean isCondition2Satisfied : ImmutableList.of(true, false)) {
        for (boolean isCondition3Satisfied : ImmutableList.of(true, false)) {
          assertWithThreeConditionsForPutIfAndDeleteIf(
              isCondition1Satisfied, isCondition2Satisfied, isCondition3Satisfied);
        }
      }
    }
  }

  /**
   * Assert if the condition is satisfied for PutIf and DeleteIf conditions
   *
   * @param operator the condition operator
   * @param compareResult to mock the comparison result between the existing record column and the
   *     condition column. Set to null to not mock the comparison call
   * @param isExistingRecordColumnNull set to true if the existing record column is null
   * @param isConditionSatisfied set to true if the condition should be satisfied
   */
  private void assertWithSingleConditionForPutIfAndDeleteIf(
      Operator operator,
      @Nullable Integer compareResult,
      boolean isExistingRecordColumnNull,
      boolean isConditionSatisfied) {
    // Act Assert
    Column<?> existingRecordColumn = mock(Column.class);
    Column<?> conditionalExpressionColumn = mock(Column.class);
    when(conditionalExpressionColumn.getName()).thenReturn(C1);
    when(existingRecord.getColumns()).thenReturn(ImmutableMap.of(C1, existingRecordColumn));
    ConditionalExpression conditionalExpression = mock(ConditionalExpression.class);
    doReturn(conditionalExpressionColumn).when(conditionalExpression).getColumn();
    when(conditionalExpression.getOperator()).thenReturn(operator);
    // Assert on a Put operation with PutIf and Delete operation with DeleteIf
    for (Mutation mutation : prepareMutationOperations(ImmutableList.of(conditionalExpression))) {
      if (isExistingRecordColumnNull) {
        when(existingRecordColumn.hasNullValue()).thenReturn(true);
      } else if (compareResult != null) {
        // mock the comparison between the existing record column and the condition column
        when(existingRecordColumn.compareTo(any())).thenReturn(compareResult);
      }
      if (isConditionSatisfied) {
        Assertions.assertThatCode(() -> validateConditionIsSatisfied(mutation, existingRecord))
            .doesNotThrowAnyException();
      } else {
        Assertions.assertThatThrownBy(() -> validateConditionIsSatisfied(mutation, existingRecord))
            .isInstanceOf(UnsatisfiedConditionException.class)
            .hasMessageContaining(C1);
      }
    }
  }

  private void assertWithThreeConditionsForPutIfAndDeleteIf(
      boolean isCondition1Satisfied, boolean isCondition2Satisfied, boolean isCondition3Satisfied) {
    Column<?> existingRecordColumn1 = mock(Column.class);
    Column<?> existingRecordColumn2 = mock(Column.class);
    Column<?> existingRecordColumn3 = mock(Column.class);
    when(existingRecord.getColumns())
        .thenReturn(
            ImmutableMap.of(
                C1, existingRecordColumn1, C2, existingRecordColumn2, C3, existingRecordColumn3));
    // The first condition operator is 'Equal'
    ConditionalExpression conditionalExpression1 = mock(ConditionalExpression.class);
    Column<?> conditionalExpressionColumn1 = mock(Column.class);
    when(conditionalExpressionColumn1.getName()).thenReturn(C1);
    doReturn(conditionalExpressionColumn1).when(conditionalExpression1).getColumn();
    when(conditionalExpression1.getOperator()).thenReturn(Operator.EQ);
    if (isCondition1Satisfied) {
      when(existingRecordColumn1.compareTo(any())).thenReturn(0);
    } else {
      when(existingRecordColumn1.compareTo(any())).thenReturn(-1);
    }
    // The second condition operator is 'GreaterThan'
    ConditionalExpression conditionalExpression2 = mock(ConditionalExpression.class);
    Column<?> conditionalExpressionColumn2 = mock(Column.class);
    when(conditionalExpressionColumn2.getName()).thenReturn(C2);
    doReturn(conditionalExpressionColumn2).when(conditionalExpression2).getColumn();
    when(conditionalExpression2.getOperator()).thenReturn(Operator.GT);
    if (isCondition2Satisfied) {
      when(existingRecordColumn2.compareTo(any())).thenReturn(1);
    } else {
      when(existingRecordColumn2.compareTo(any())).thenReturn(-1);
    }
    // The third condition operator is 'IsNull'
    ConditionalExpression conditionalExpression3 = mock(ConditionalExpression.class);
    Column<?> conditionalExpressionColumn3 = mock(Column.class);
    when(conditionalExpressionColumn3.getName()).thenReturn(C3);
    doReturn(conditionalExpressionColumn3).when(conditionalExpression3).getColumn();
    when(conditionalExpression3.getOperator()).thenReturn(Operator.IS_NULL);
    if (isCondition3Satisfied) {
      when(existingRecordColumn3.hasNullValue()).thenReturn(true);
    } else {
      when(existingRecordColumn3.hasNullValue()).thenReturn(false);
    }
    // Assert on a Put operation with PutIf and Delete operation with DeleteIf
    for (Mutation mutation :
        prepareMutationOperations(
            ImmutableList.of(
                conditionalExpression1, conditionalExpression2, conditionalExpression3))) {
      boolean areAllConditionsSatisfied =
          isCondition1Satisfied && isCondition2Satisfied && isCondition3Satisfied;
      if (areAllConditionsSatisfied) {
        Assertions.assertThatCode(() -> validateConditionIsSatisfied(mutation, existingRecord))
            .doesNotThrowAnyException();
      } else {
        try {
          validateConditionIsSatisfied(mutation, existingRecord);
        } catch (Exception e) {
          assertThat(e).isInstanceOf(UnsatisfiedConditionException.class);
          if (!isCondition1Satisfied) {
            assertThat(e.getMessage()).contains(C1).doesNotContain(C2, C3);
          } else if (!isCondition2Satisfied) {
            assertThat(e.getMessage()).contains(C2).doesNotContain(C1, C3);
          } else if (!isCondition3Satisfied) {
            assertThat(e.getMessage()).contains(C3).doesNotContain(C1, C2);
          }
        }
      }
    }
  }

  private List<Mutation> prepareMutationOperations(
      List<ConditionalExpression> conditionalExpressions) {
    Put put = mock(Put.class);
    PutIf putIf = ConditionBuilder.putIf(conditionalExpressions);
    when(put.getCondition()).thenReturn(Optional.of(putIf));
    Delete delete = mock(Delete.class);
    DeleteIf deleteIf = ConditionBuilder.deleteIf(conditionalExpressions);
    when(delete.getCondition()).thenReturn(Optional.of(deleteIf));
    return ImmutableList.of(put, delete);
  }

  private void validateConditionIsSatisfied(Mutation mutation, TransactionResult existingRecord)
      throws UnsatisfiedConditionException {
    if (mutation instanceof Put) {
      validator.checkIfConditionIsSatisfied((Put) mutation, existingRecord);
    } else {
      validator.checkIfConditionIsSatisfied((Delete) mutation, existingRecord);
    }
  }
}
