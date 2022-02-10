package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Put;
import com.scalar.db.io.Key;
import org.junit.Test;

public class PartitionedMutationsKeyTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_3 = "text3";

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Put(partitionKey, null).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private Put prepareAnotherPut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_3);
    return new Put(partitionKey, null).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  @Test
  public void equals_SameOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(put);

    // Act
    boolean res = key.equals(new PartitionedMutations.Key(put));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_EquivalentOperationGivenInConstructor_ShouldReturnTrue() {
    // Arrange
    Put one = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(one);
    Put another = preparePut();

    // Act
    boolean res = key.equals(new PartitionedMutations.Key(another));

    // Assert
    assertThat(res).isTrue();
  }

  @Test
  public void equals_NonEquivalentOperationGivenInConstructor_ShouldReturnFalse() {
    // Arrange
    Put one = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(one);
    Put another = prepareAnotherPut();

    // Act
    boolean res = key.equals(new PartitionedMutations.Key(another));

    // Assert
    assertThat(res).isFalse();
  }

  @Test
  public void compareTo_SameOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Put put = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(put);

    // Act
    int res = key.compareTo(new PartitionedMutations.Key(put));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_EquivalentOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Put one = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(one);
    Put another = preparePut();

    // Act
    int res = key.compareTo(new PartitionedMutations.Key(another));

    // Assert
    assertThat(res).isEqualTo(0);
  }

  @Test
  public void compareTo_BiggerOperationGivenInConstructor_ShouldReturnNegative() {
    // Arrange
    Put one = preparePut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(one);
    Put another = prepareAnotherPut();

    // Act
    int res = key.compareTo(new PartitionedMutations.Key(another));

    // Assert
    assertThat(res).isLessThan(0);
  }

  @Test
  public void compareTo_LesserOperationGivenInConstructor_ShouldReturnZero() {
    // Arrange
    Put one = prepareAnotherPut();
    PartitionedMutations.Key key = new PartitionedMutations.Key(one);
    Put another = preparePut();

    // Act
    int res = key.compareTo(new PartitionedMutations.Key(another));

    // Assert
    assertThat(res).isGreaterThan(0);
  }
}
