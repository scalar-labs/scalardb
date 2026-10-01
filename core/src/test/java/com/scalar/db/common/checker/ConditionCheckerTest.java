package com.scalar.db.common.checker;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConditionCheckerTest {
  private static final String PK_COL = "pk";
  private static final String COL = "col";
  private ConditionChecker checker;

  @BeforeEach
  public void setUp() {
    checker =
        new ConditionChecker(
            TableMetadata.newBuilder()
                .addColumn(PK_COL, DataType.INT)
                .addColumn(COL, DataType.INT)
                .addPartitionKey(PK_COL)
                .build());
  }

  @Test
  public void check_PutIfWithoutExpressions_ShouldReturnFalse() {
    // Arrange
    PutIf condition = ConditionBuilder.putIf(Collections.emptyList());

    // Act
    boolean actual = checker.check(condition, true);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void check_DeleteIfWithoutExpressions_ShouldReturnFalse() {
    // Arrange
    DeleteIf condition = ConditionBuilder.deleteIf(Collections.emptyList());

    // Act
    boolean actual = checker.check(condition, false);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void check_PutIfWithoutExpressionsAfterValidCondition_ShouldReturnFalse() {
    // Arrange
    PutIf validCondition =
        ConditionBuilder.putIf(ConditionBuilder.column(COL).isEqualToInt(1)).build();
    PutIf emptyCondition = ConditionBuilder.putIf(Collections.emptyList());
    assertThat(checker.check(validCondition, true)).isTrue();

    // Act
    boolean actual = checker.check(emptyCondition, true);

    // Assert
    assertThat(actual).isFalse();
  }
}
