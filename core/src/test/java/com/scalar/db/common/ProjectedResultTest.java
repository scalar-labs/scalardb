package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProjectedResultTest {

  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ACCOUNT_ID, DataType.INT)
          .addColumn(ACCOUNT_TYPE, DataType.INT)
          .addColumn(BALANCE, DataType.INT)
          .addPartitionKey(ACCOUNT_ID)
          .addClusteringKey(ACCOUNT_TYPE)
          .build();

  private static final IntColumn ACCOUNT_ID_COLUMN = IntColumn.of(ACCOUNT_ID, 0);
  private static final IntColumn ACCOUNT_TYPE_COLUMN = IntColumn.of(ACCOUNT_TYPE, 1);
  private static final IntColumn BALANCE_COLUMN = IntColumn.of(BALANCE, 2);

  private Result result;

  @BeforeEach
  public void setUp() {
    // Arrange
    Map<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ACCOUNT_ID, ACCOUNT_ID_COLUMN)
            .put(ACCOUNT_TYPE, ACCOUNT_TYPE_COLUMN)
            .put(BALANCE, BALANCE_COLUMN)
            .build();

    result = new ResultImpl(columns, TABLE_METADATA);
  }

  @Test
  public void withoutProjections_ShouldContainAllColumns() {
    // Arrange
    ProjectedResult projectedResult = new ProjectedResult(result, Collections.emptyList());

    // Act Assert
    assertThat(projectedResult.getColumns().keySet())
        .containsOnly(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE);
    assertThat(projectedResult.getColumns().values())
        .containsOnly(ACCOUNT_ID_COLUMN, ACCOUNT_TYPE_COLUMN, BALANCE_COLUMN);
    assertThat(projectedResult.getContainedColumnNames())
        .containsOnly(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE);

    assertThat(projectedResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(projectedResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(projectedResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(projectedResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(projectedResult.contains(ACCOUNT_TYPE)).isTrue();
    assertThat(projectedResult.isNull(ACCOUNT_TYPE)).isFalse();
    assertThat(projectedResult.getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(projectedResult.getAsObject(ACCOUNT_TYPE)).isEqualTo(1);

    assertThat(projectedResult.contains(BALANCE)).isTrue();
    assertThat(projectedResult.isNull(BALANCE)).isFalse();
    assertThat(projectedResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(projectedResult.getAsObject(BALANCE)).isEqualTo(2);
  }

  @Test
  public void withProjections_ShouldContainProjectedColumns() {
    // Arrange
    ProjectedResult projectedResult =
        new ProjectedResult(result, Arrays.asList(ACCOUNT_ID, BALANCE));

    // Act Assert
    assertThat(projectedResult.getColumns().keySet()).containsOnly(ACCOUNT_ID, BALANCE);
    assertThat(projectedResult.getColumns().values())
        .containsOnly(ACCOUNT_ID_COLUMN, BALANCE_COLUMN);
    assertThat(projectedResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Arrays.asList(ACCOUNT_ID, BALANCE)));

    assertThat(projectedResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(projectedResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(projectedResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(projectedResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(projectedResult.contains(ACCOUNT_TYPE)).isFalse();

    assertThat(projectedResult.contains(BALANCE)).isTrue();
    assertThat(projectedResult.isNull(BALANCE)).isFalse();
    assertThat(projectedResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(projectedResult.getAsObject(BALANCE)).isEqualTo(2);
  }

  @Test
  public void equals_SameResultGiven_WithoutProjections_ShouldReturnTrue() {
    // Arrange
    ProjectedResult projectedResult = new ProjectedResult(result, Collections.emptyList());
    ProjectedResult anotherFilterResult = new ProjectedResult(result, Collections.emptyList());

    // Act
    boolean isEqual = projectedResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentResiltGiven_WithoutProjections_ShouldReturnFalse() {
    // Arrange
    ProjectedResult projectedResult = new ProjectedResult(result, Collections.emptyList());
    ProjectedResult anotherFilterResult =
        new ProjectedResult(
            new ResultImpl(Collections.emptyMap(), TABLE_METADATA), Collections.emptyList());

    // Act
    boolean isEqual = projectedResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_SameResultGiven_WithProjections_ShouldReturnTrue() {
    // Arrange
    ProjectedResult projectedResult =
        new ProjectedResult(result, Collections.singletonList(BALANCE));
    ProjectedResult anotherFilterResult =
        new ProjectedResult(result, Collections.singletonList(BALANCE));

    // Act
    boolean isEqual = projectedResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_WithoutProjections_ShouldReturnTrue() {
    // Arrange
    Result projectedResult = new ProjectedResult(result, Collections.emptyList());
    Result anotherResult =
        new ResultImpl(
            ImmutableMap.of(
                ACCOUNT_ID,
                ACCOUNT_ID_COLUMN,
                ACCOUNT_TYPE,
                ACCOUNT_TYPE_COLUMN,
                BALANCE,
                BALANCE_COLUMN),
            TABLE_METADATA);

    // Act
    boolean isEqual = projectedResult.equals(anotherResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_WithProjections_ShouldReturnFalse() {
    // Arrange
    Result projectedResult = new ProjectedResult(result, Collections.singletonList(BALANCE));

    // Act
    boolean isEqual = projectedResult.equals(result);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_ResultImplWithDifferentValuesGiven_WithSameProjections_ShouldReturnTrue() {
    // Arrange
    Result projectedResult = new ProjectedResult(result, Collections.singletonList(BALANCE));
    Result anotherResult = new ResultImpl(ImmutableMap.of(BALANCE, BALANCE_COLUMN), TABLE_METADATA);

    // Act
    boolean isEqual = projectedResult.equals(anotherResult);

    // Assert
    assertThat(isEqual).isTrue();
  }
}
