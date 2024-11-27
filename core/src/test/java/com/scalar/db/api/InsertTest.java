package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class InsertTest {
  private static final String ANY_NAMESPACE = "ns";
  private static final String ANY_TABLE = "tbl";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";

  private Insert prepareInsert() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Insert.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .textValue(ANY_NAME_1, ANY_TEXT_1)
        .textValue(ANY_NAME_2, ANY_TEXT_2)
        .build();
  }

  @Test
  public void getPartitionKey_ProperKeyGiven_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(expected)
            .clusteringKey(clusteringKey)
            .build();

    // Act
    Key actual = insert.getPartitionKey();

    // Assert
    Assertions.<Key>assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGiven_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key expected = Key.ofText(ANY_NAME_1, ANY_TEXT_2);
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(expected)
            .build();

    // Act
    Optional<Key> actual = insert.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGiven_ShouldReturnNull() {
    // Arrange
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(partitionKey)
            .build();

    // Act
    Optional<Key> actual = insert.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void getColumns_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Insert insert = prepareInsert();

    // Act Assert
    Map<String, Column<?>> values = insert.getColumns();
    assertThatThrownBy(() -> values.put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Insert insert = prepareInsert();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = insert.equals(insert);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameInsertGiven_ShouldReturnTrue() {
    // Arrange
    Insert insert = prepareInsert();
    Insert another = prepareInsert();

    // Act
    boolean ret = insert.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(insert.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_InsertWithDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Insert insert = prepareInsert();
    Insert another =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_1, ANY_TEXT_1)
            .textValue(ANY_NAME_2, ANY_TEXT_3)
            .build();

    // Act
    boolean ret = insert.equals(another);

    // Assert
    assertThat(ret).isFalse();
    assertThat(insert.hashCode()).isNotEqualTo(another.hashCode());
  }

  @Test
  public void getAttribute_ShouldReturnProperValues() {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act Assert
    assertThat(insert.getAttribute("a1")).hasValue("v1");
    assertThat(insert.getAttribute("a2")).hasValue("v2");
    assertThat(insert.getAttribute("a3")).hasValue("v3");
    assertThat(insert.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }
}
