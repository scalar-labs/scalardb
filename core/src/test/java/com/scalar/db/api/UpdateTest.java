package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class UpdateTest {
  private static final String ANY_NAMESPACE = "ns";
  private static final String ANY_TABLE = "tbl";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Update prepareUpdate() {
    return Update.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .textValue(ANY_NAME_1, ANY_TEXT_1)
        .textValue(ANY_NAME_2, ANY_TEXT_2)
        .build();
  }

  @Test
  public void getPartitionKey_ProperKeyGiven_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(expected)
            .clusteringKey(clusteringKey)
            .build();

    // Act
    Key actual = update.getPartitionKey();

    // Assert
    Assertions.<Key>assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGiven_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key expected = Key.ofText(ANY_NAME_1, ANY_TEXT_2);
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(expected)
            .build();

    // Act
    Optional<Key> actual = update.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGiven_ShouldReturnNull() {
    // Arrange
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(partitionKey)
            .build();

    // Act
    Optional<Key> actual = update.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void getColumns_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Update update = prepareUpdate();

    // Act Assert
    Map<String, Column<?>> values = update.getColumns();
    assertThatThrownBy(() -> values.put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Update update = prepareUpdate();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = update.equals(update);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SamePutGiven_ShouldReturnTrue() {
    // Arrange
    Update update = prepareUpdate();
    Update another = prepareUpdate();

    // Act
    boolean ret = update.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(update.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_UpdateWithDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Update update = prepareUpdate();
    Update another =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_1, ANY_TEXT_1)
            .textValue(ANY_NAME_2, ANY_TEXT_3)
            .build();

    // Act
    boolean ret = update.equals(another);

    // Assert
    assertThat(ret).isFalse();
    assertThat(update.hashCode()).isNotEqualTo(another.hashCode());
  }

  @Test
  public void equals_UpdateWithDifferentConditionsGiven_ShouldReturnFalse() {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();
    Update another =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                    .build())
            .build();
    // Act
    boolean ret = update.equals(another);

    // Assert
    assertThat(ret).isFalse();
    assertThat(update.hashCode()).isNotEqualTo(another.hashCode());
  }
}
