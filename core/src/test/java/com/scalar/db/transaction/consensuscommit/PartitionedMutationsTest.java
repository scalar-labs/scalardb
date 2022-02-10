package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class PartitionedMutationsTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TEXT_5 = "text5";
  private static final String ANY_TEXT_6 = "text6";

  private Put preparePut(Key partitionKey, Key clusteringKey) {
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private List<Put> preparePuts1() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey1 = new Key(ANY_NAME_2, ANY_TEXT_2);
    Key clusteringKey2 = new Key(ANY_NAME_3, ANY_TEXT_3);
    Put put1 = preparePut(partitionKey, clusteringKey1);
    Put put2 = preparePut(partitionKey, clusteringKey2);
    return Arrays.asList(put1, put2);
  }

  private List<Put> preparePuts2() {
    Key partitionKey = new Key(ANY_NAME_4, ANY_TEXT_4);
    Key clusteringKey1 = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey2 = new Key(ANY_NAME_6, ANY_TEXT_6);
    Put put1 = preparePut(partitionKey, clusteringKey1);
    Put put2 = preparePut(partitionKey, clusteringKey2);
    return Arrays.asList(put1, put2);
  }

  @Test
  public void
      get_MutationCollectionsAndPartitionGiven_ShouldReturnMutationsForSpecifiedPartition() {
    // Arrange
    List<Put> puts = preparePuts1();
    PartitionedMutations partitionedMutations = new PartitionedMutations(puts);

    // Act
    PartitionedMutations.Key key = new PartitionedMutations.Key(puts.get(0));
    List<Mutation> mutations = partitionedMutations.get(key);

    // Assert
    assertThat(mutations.size()).isEqualTo(puts.size());
    assertThat(mutations.contains(puts.get(0))).isTrue();
    assertThat(mutations.contains(puts.get(1))).isTrue();
  }

  @Test
  public void getOrderedKeys_MutationCollectionsGiven_ShouldReturnOrderedKeys() {
    // Arrange
    List<Put> puts1 = preparePuts1();
    List<Put> puts2 = preparePuts2();
    PartitionedMutations partitionedMutations = new PartitionedMutations(puts1, puts2);

    // Act
    List<PartitionedMutations.Key> keys = partitionedMutations.getOrderedKeys();

    // Assert
    assertThat(keys.size()).isEqualTo(2);
    assertThat(keys.get(0)).isEqualTo(new PartitionedMutations.Key(puts1.get(0)));
    assertThat(keys.get(1)).isEqualTo(new PartitionedMutations.Key(puts2.get(0)));
  }

  @Test
  public void
      getOrderedKeys_MutationCollectionsGivenInDifferentOrder_ShouldReturnEquallyOrderedKeys() {
    // Arrange
    List<Put> puts1 = preparePuts1();
    List<Put> puts2 = preparePuts2();
    PartitionedMutations partitionedMutations1 = new PartitionedMutations(puts1, puts2);
    PartitionedMutations partitionedMutations2 = new PartitionedMutations(puts2, puts1);

    // Act
    List<PartitionedMutations.Key> keys1 = partitionedMutations1.getOrderedKeys();
    List<PartitionedMutations.Key> keys2 = partitionedMutations2.getOrderedKeys();

    // Assert
    Assertions.assertThat(keys1).isEqualTo(keys2);
    assertThat(partitionedMutations1).isEqualTo(partitionedMutations2);
  }
}
