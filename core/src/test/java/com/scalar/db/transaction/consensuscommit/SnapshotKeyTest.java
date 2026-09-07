package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.Get;
import com.scalar.db.io.CollationComparators;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

public class SnapshotKeyTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Get prepareGetWithPartitionKeyText(String partitionKeyValue) {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, partitionKeyValue))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .build();
  }

  private Get prepareGetWithPartitionKeyInt(int partitionKeyValue) {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofInt(ANY_NAME_1, partitionKeyValue))
        .build();
  }

  private Get prepareGetWithClusteringKeyColumn(TextColumn clusteringKeyColumn) {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.newBuilder().add(clusteringKeyColumn).build())
        .build();
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Get prepareGetWithoutClusteringKey() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private Get prepareAnotherGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_3);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_4);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  @Test
  public void equals_SameOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, CollationComparators.BINARY);

    // Act
    boolean res = key.equals(new Snapshot.Key(get, CollationComparators.BINARY));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_EquivalentOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_NonEquivalentOperationGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void equals_EquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGetWithoutClusteringKey();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void
      equals_NonEquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void compareTo_SameOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, CollationComparators.BINARY);

    // Act
    int res = key.compareTo(new Snapshot.Key(get, CollationComparators.BINARY));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_EquivalentOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_BiggerOperationGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareAnotherGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void compareTo_LesserOperationGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareAnotherGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isGreaterThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithClusteringKeyGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithoutClusteringKeyGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, CollationComparators.BINARY);
    Get another = prepareGetWithoutClusteringKey();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, CollationComparators.BINARY));

    // Assert
    assertThat(res).isGreaterThan(0);
  }

  @Test
  public void
      equalsAndHashCode_CaseVariantTextKeysUnderIcuPrimary_ShouldBeEqualAndHitSameMapEntry() {
    // Arrange
    Snapshot.Key keyUpper =
        new Snapshot.Key(
            prepareGetWithPartitionKeyText("Apple"), CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key keyLower =
        new Snapshot.Key(
            prepareGetWithPartitionKeyText("apple"), CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(keyUpper).isEqualTo(keyLower);
    assertThat(keyLower).isEqualTo(keyUpper);
    assertThat(keyUpper.hashCode()).isEqualTo(keyLower.hashCode());

    ConcurrentHashMap<Snapshot.Key, String> map = new ConcurrentHashMap<>();
    map.put(keyUpper, "first");
    map.put(keyLower, "second");
    assertThat(map).hasSize(1);
    assertThat(map.get(keyUpper)).isEqualTo("second");
    assertThat(map.get(keyLower)).isEqualTo("second");
  }

  @Test
  public void compareTo_CaseVariantTextKeysUnderIcuPrimary_ShouldReturnZero() {
    // Arrange
    Snapshot.Key keyUpper =
        new Snapshot.Key(
            prepareGetWithPartitionKeyText("Apple"), CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key keyLower =
        new Snapshot.Key(
            prepareGetWithPartitionKeyText("apple"), CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(keyUpper.compareTo(keyLower)).isEqualTo(0);
    assertThat(keyLower.compareTo(keyUpper)).isEqualTo(0);
  }

  @Test
  public void equals_CaseVariantTextKeysUnderBinary_ShouldStayDistinct() {
    // Arrange
    Snapshot.Key keyUpper =
        new Snapshot.Key(prepareGetWithPartitionKeyText("Apple"), CollationComparators.BINARY);
    Snapshot.Key keyLower =
        new Snapshot.Key(prepareGetWithPartitionKeyText("apple"), CollationComparators.BINARY);

    // Act Assert
    assertThat(keyUpper).isNotEqualTo(keyLower);
    ConcurrentHashMap<Snapshot.Key, String> map = new ConcurrentHashMap<>();
    map.put(keyUpper, "first");
    map.put(keyLower, "second");
    assertThat(map).hasSize(2);
  }

  @Test
  public void equals_NonTextKeysUnderIcuPrimary_ShouldBehaveByteExact() {
    // Arrange
    Snapshot.Key keyOne =
        new Snapshot.Key(
            prepareGetWithPartitionKeyInt(1), CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key keyOneAgain =
        new Snapshot.Key(
            prepareGetWithPartitionKeyInt(1), CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key keyTwo =
        new Snapshot.Key(
            prepareGetWithPartitionKeyInt(2), CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(keyOne).isEqualTo(keyOneAgain);
    assertThat(keyOne.hashCode()).isEqualTo(keyOneAgain.hashCode());
    assertThat(keyOne).isNotEqualTo(keyTwo);
  }

  @Test
  public void equals_NullTextClusteringValueUnderIcuPrimary_ShouldBeHandledWithoutNpe() {
    // Arrange
    Snapshot.Key keyNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.ofNull(ANY_NAME_2)),
            CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key anotherKeyNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.ofNull(ANY_NAME_2)),
            CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key keyNonNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.of(ANY_NAME_2, ANY_TEXT_2)),
            CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThatCode(keyNull::hashCode).doesNotThrowAnyException();
    assertThat(keyNull).isEqualTo(anotherKeyNull);
    assertThat(keyNull.hashCode()).isEqualTo(anotherKeyNull.hashCode());
    assertThat(keyNull).isNotEqualTo(keyNonNull);
    assertThat(keyNonNull).isNotEqualTo(keyNull);
  }

  @Test
  public void toString_TextKeyUnderIcuPrimary_ShouldShowOriginalBytes() {
    // Arrange
    Snapshot.Key key =
        new Snapshot.Key(
            prepareGetWithPartitionKeyText("Apple"), CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(key.toString()).contains("Apple");
  }

  private Get prepareGetWithCompositePartitionKey(String value1, String value2) {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(
            Key.newBuilder().addText(ANY_NAME_1, value1).addText(ANY_NAME_2, value2).build())
        .build();
  }

  @Test
  public void
      equals_CompositePartitionKeyVsSplitPartitionAndClusteringKeyUnderIcuPrimary_ShouldStayDistinct() {
    // Arrange
    Snapshot.Key compositeKey =
        new Snapshot.Key(
            prepareGetWithCompositePartitionKey("a", "b"),
            CollationComparators.CASE_INSENSITIVE_ICU);
    Get split =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, "a"))
            .clusteringKey(Key.ofText(ANY_NAME_2, "b"))
            .build();
    Snapshot.Key splitKey = new Snapshot.Key(split, CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(compositeKey).isNotEqualTo(splitKey);
    assertThat(splitKey).isNotEqualTo(compositeKey);
    ConcurrentHashMap<Snapshot.Key, String> map = new ConcurrentHashMap<>();
    map.put(compositeKey, "composite");
    map.put(splitKey, "split");
    assertThat(map).hasSize(2);
    assertThat(map.get(compositeKey)).isEqualTo("composite");
    assertThat(map.get(splitKey)).isEqualTo("split");
  }

  @Test
  public void
      equalsAndHashCode_CaseVariantCompositeTextPartitionKeysUnderIcuPrimary_ShouldBeEqual() {
    // Arrange
    Snapshot.Key key1 =
        new Snapshot.Key(
            prepareGetWithCompositePartitionKey("a", "B"),
            CollationComparators.CASE_INSENSITIVE_ICU);
    Snapshot.Key key2 =
        new Snapshot.Key(
            prepareGetWithCompositePartitionKey("A", "b"),
            CollationComparators.CASE_INSENSITIVE_ICU);

    // Act Assert
    assertThat(key1).isEqualTo(key2);
    assertThat(key2).isEqualTo(key1);
    assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    ConcurrentHashMap<Snapshot.Key, String> map = new ConcurrentHashMap<>();
    map.put(key1, "first");
    map.put(key2, "second");
    assertThat(map).hasSize(1);
    assertThat(map.get(key1)).isEqualTo("second");
  }
}
