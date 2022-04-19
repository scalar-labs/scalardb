package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.io.IntValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class ConditionExpressionBuilderTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final int ANY_INT = 1;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT);

  @BeforeEach
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
        ConditionBuilder.putIf(
                ConditionBuilder.column(ANY_NAME_1).isEqualToInt(ANY_INT_VALUE.getAsInt()))
            .and(ConditionBuilder.column(ANY_NAME_2).isGreaterThanInt(ANY_INT_VALUE.getAsInt()))
            .and(ConditionBuilder.column(ANY_NAME_3).isNullInt())
            .and(ConditionBuilder.column(ANY_NAME_4).isNotNullInt())
            .build();
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "#ccol0 = :cval0 AND #ccol1 > :cval1 "
                + "AND (attribute_not_exists(#ccol2) OR #ccol2 = :cval2) "
                + "AND (attribute_exists(#ccol3) AND NOT #ccol3 = :cval3)");
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
        ConditionBuilder.deleteIf(
                ConditionBuilder.column(ANY_NAME_1).isEqualToInt(ANY_INT_VALUE.getAsInt()))
            .and(ConditionBuilder.column(ANY_NAME_2).isGreaterThanInt(ANY_INT_VALUE.getAsInt()))
            .and(ConditionBuilder.column(ANY_NAME_3).isNullInt())
            .and(ConditionBuilder.column(ANY_NAME_4).isNotNullInt())
            .build();

    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS, DynamoOperation.CONDITION_VALUE_ALIAS);

    // Act
    condition.accept(builder);
    String actual = builder.build();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "#ccol0 = :cval0 AND #ccol1 > :cval1 "
                + "AND (attribute_not_exists(#ccol2) OR #ccol2 = :cval2) "
                + "AND (attribute_exists(#ccol3) AND NOT #ccol3 = :cval3)");
  }
}
