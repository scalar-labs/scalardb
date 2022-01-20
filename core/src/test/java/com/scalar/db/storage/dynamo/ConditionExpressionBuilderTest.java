package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.io.IntValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class ConditionExpressionBuilderTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final int ANY_INT = 1;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT);

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void build_NoConditionsGiven_ShouldReturnEmpty() {
    // Arrange
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    String actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo("");
  }

  @Test
  public void build_PutIfAcceptCalled_ShouldReturnCondition() {
    // Arrange
    PutIf condition =
        new PutIf(
            new ConditionalExpression(ANY_NAME_1, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_2, ANY_INT_VALUE, Operator.GT));
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo("#ccol0 = :cval0 AND #ccol1 > :cval1");
  }

  @Test
  public void visit_PutIfExistsAcceptCalled_ShouldReturnEmpty() {
    // Arrange
    PutIfExists condition = new PutIfExists();
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo("");
  }

  @Test
  public void visit_PutIfNotExistsAcceptCalled_ShouldReturnEmpty() {
    // Arrange
    PutIfNotExists condition = new PutIfNotExists();
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo("");
  }

  @Test
  public void visit_DeleteIfAcceptCalled_ShouldCallWhere() {
    // Arrange
    DeleteIf condition =
        new DeleteIf(
            new ConditionalExpression(ANY_NAME_1, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_2, ANY_INT_VALUE, Operator.GT));
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo("#ccol0 = :cval0 AND #ccol1 > :cval1");
  }
}
