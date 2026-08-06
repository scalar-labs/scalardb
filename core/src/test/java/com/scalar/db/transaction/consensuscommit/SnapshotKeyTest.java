package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.Get;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.Properties;
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

  private static CollationComparator binaryCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    return CollationComparator.from(new DatabaseConfig(props));
  }

  private static CollationComparator icuPrimaryCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.COLLATION, "ICU");
    props.setProperty(DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY");
    return CollationComparator.from(new DatabaseConfig(props));
  }

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
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());

    // Act
    boolean res = key.equals(new Snapshot.Key(get, binaryCollation()));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_EquivalentOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_NonEquivalentOperationGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void equals_EquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGetWithoutClusteringKey();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void
      equals_NonEquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void compareTo_SameOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());

    // Act
    int res = key.compareTo(new Snapshot.Key(get, binaryCollation()));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_EquivalentOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_BiggerOperationGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareAnotherGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void compareTo_LesserOperationGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareAnotherGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isGreaterThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithClusteringKeyGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithoutClusteringKeyGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one, binaryCollation());
    Get another = prepareGetWithoutClusteringKey();

    // Act
    int res = key.compareTo(new Snapshot.Key(another, binaryCollation()));

    // Assert
    assertThat(res).isGreaterThan(0);
  }

  // ---- Collation-canonical identity under an ICU comparator (increment B) ----

  @Test
  public void
      equalsAndHashCode_CaseVariantTextKeysUnderIcuPrimary_ShouldBeEqualAndHitSameMapEntry() {
    // Arrange: 'Apple' and 'apple' collate-equal at PRIMARY strength.
    Snapshot.Key keyUpper =
        new Snapshot.Key(prepareGetWithPartitionKeyText("Apple"), icuPrimaryCollation());
    Snapshot.Key keyLower =
        new Snapshot.Key(prepareGetWithPartitionKeyText("apple"), icuPrimaryCollation());

    // Act Assert: equal keys with equal hash codes ...
    assertThat(keyUpper).isEqualTo(keyLower);
    assertThat(keyLower).isEqualTo(keyUpper);
    assertThat(keyUpper.hashCode()).isEqualTo(keyLower.hashCode());

    // ... that hit the SAME ConcurrentHashMap entry via either spelling.
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
        new Snapshot.Key(prepareGetWithPartitionKeyText("Apple"), icuPrimaryCollation());
    Snapshot.Key keyLower =
        new Snapshot.Key(prepareGetWithPartitionKeyText("apple"), icuPrimaryCollation());

    // Act Assert: compareTo is consistent with equals — collate-equal keys compare as 0.
    assertThat(keyUpper.compareTo(keyLower)).isEqualTo(0);
    assertThat(keyLower.compareTo(keyUpper)).isEqualTo(0);
  }

  @Test
  public void equals_CaseVariantTextKeysUnderBinary_ShouldStayDistinct() {
    // Arrange
    Snapshot.Key keyUpper =
        new Snapshot.Key(prepareGetWithPartitionKeyText("Apple"), binaryCollation());
    Snapshot.Key keyLower =
        new Snapshot.Key(prepareGetWithPartitionKeyText("apple"), binaryCollation());

    // Act Assert: under BINARY, identity stays byte-exact.
    assertThat(keyUpper).isNotEqualTo(keyLower);
    ConcurrentHashMap<Snapshot.Key, String> map = new ConcurrentHashMap<>();
    map.put(keyUpper, "first");
    map.put(keyLower, "second");
    assertThat(map).hasSize(2);
  }

  @Test
  public void equals_NonTextKeysUnderIcuPrimary_ShouldBehaveByteExact() {
    // Arrange: canonicalization only applies to TEXT key columns; INT keys are untouched.
    Snapshot.Key keyOne = new Snapshot.Key(prepareGetWithPartitionKeyInt(1), icuPrimaryCollation());
    Snapshot.Key keyOneAgain =
        new Snapshot.Key(prepareGetWithPartitionKeyInt(1), icuPrimaryCollation());
    Snapshot.Key keyTwo = new Snapshot.Key(prepareGetWithPartitionKeyInt(2), icuPrimaryCollation());

    // Act Assert
    assertThat(keyOne).isEqualTo(keyOneAgain);
    assertThat(keyOne.hashCode()).isEqualTo(keyOneAgain.hashCode());
    assertThat(keyOne).isNotEqualTo(keyTwo);
  }

  @Test
  public void equals_NullTextClusteringValueUnderIcuPrimary_ShouldBeHandledWithoutNpe() {
    // Arrange: null TEXT key values have no canonical form; they fall back to column equality.
    Snapshot.Key keyNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.ofNull(ANY_NAME_2)),
            icuPrimaryCollation());
    Snapshot.Key anotherKeyNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.ofNull(ANY_NAME_2)),
            icuPrimaryCollation());
    Snapshot.Key keyNonNull =
        new Snapshot.Key(
            prepareGetWithClusteringKeyColumn(TextColumn.of(ANY_NAME_2, ANY_TEXT_2)),
            icuPrimaryCollation());

    // Act Assert: no NPE anywhere; two null-valued keys are equal, null vs non-null is not.
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
        new Snapshot.Key(prepareGetWithPartitionKeyText("Apple"), icuPrimaryCollation());

    // Act Assert: toString keeps the original spelling, not the canonical collation form.
    assertThat(key.toString()).contains("Apple");
  }
}
