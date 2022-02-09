package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Get;
import com.scalar.db.io.Key;
import org.junit.Test;

public class SnapshotKeyTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareGetWithoutClusteringKey() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Get(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private Get prepareAnotherGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_3);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_4);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void equals_SameOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    // Act
    boolean res = key.equals(new Snapshot.Key(get));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_EquivalentOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_NonEquivalentOperationGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void equals_EquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGetWithoutClusteringKey();

    // Act
    boolean res = key.equals(new Snapshot.Key(another));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void
      equals_NonEquivalentOperationWithoutClusteringKeyGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareAnotherGet();

    // Act
    boolean res = key.equals(new Snapshot.Key(another));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void compareTo_SameOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    // Act
    int res = key.compareTo(new Snapshot.Key(get));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_EquivalentOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_BiggerOperationGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareAnotherGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void compareTo_LesserOperationGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareAnotherGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another));

    // Assert
    assertThat(res).isGreaterThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithClusteringKeyGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Get one = prepareGetWithoutClusteringKey();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGet();

    // Act
    int res = key.compareTo(new Snapshot.Key(another));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void
      compareTo_SameOperationExceptWithoutClusteringKeyGivenInConstructor_ShouldReturnPositive() {
    // Arrange
    Get one = prepareGet();
    Snapshot.Key key = new Snapshot.Key(one);
    Get another = prepareGetWithoutClusteringKey();

    // Act
    int res = key.compareTo(new Snapshot.Key(another));

    // Assert
    assertThat(res).isGreaterThan(0);
  }
}
