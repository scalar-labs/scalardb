package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MutationsGrouperTest {

  @Mock private StorageInfoProvider storageInfoProvider;

  private MutationsGrouper grouper;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    grouper = new MutationsGrouper(storageInfoProvider);
  }

  @Test
  public void groupMutations_WithEmptyCollection_ShouldReturnEmptyList() throws ExecutionException {
    // Act
    List<List<Mutation>> result = grouper.groupMutations(Collections.emptyList());

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void groupMutations_WithRecordAtomicity_ShouldGroupCorrectly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.RECORD);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Three mutations with different records
    Key partitionKey1 = mock(Key.class);
    Key clusteringKey1 = mock(Key.class);
    Mutation mutation1 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));

    Key partitionKey2 = mock(Key.class);
    Key clusteringKey2 = mock(Key.class);
    Mutation mutation2 =
        createMutation(namespace, table, partitionKey2, Optional.of(clusteringKey2));

    Key clusteringKey3 = mock(Key.class);
    Mutation mutation3 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey3));

    Mutation mutation4 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));

    Mutation mutation5 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey3));

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(
            Arrays.asList(mutation1, mutation2, mutation3, mutation4, mutation5));

    // Assert
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).containsExactly(mutation1, mutation4);
    assertThat(result.get(1)).containsExactly(mutation2);
    assertThat(result.get(2)).containsExactly(mutation3, mutation5);
  }

  @Test
  public void groupMutations_WithPartitionAtomicity_ShouldGroupCorrectly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.PARTITION);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Three mutations with two different partitions
    Key partitionKey1 = mock(Key.class);
    Key clusteringKey1 = mock(Key.class);
    Mutation mutation1 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));

    Key partitionKey2 = mock(Key.class);
    Key clusteringKey2 = mock(Key.class);
    Mutation mutation2 =
        createMutation(namespace, table, partitionKey2, Optional.of(clusteringKey2));

    Key clusteringKey3 = mock(Key.class);
    Mutation mutation3 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey3));

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(Arrays.asList(mutation1, mutation2, mutation3));

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).containsExactly(mutation1, mutation3);
    assertThat(result.get(1)).containsExactly(mutation2);
  }

  @Test
  public void groupMutations_WithTableAtomicity_ShouldGroupCorrectly() throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String table1 = "table1";
    String table2 = "table2";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo);
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo);

    // Three mutations with two different tables
    Key partitionKey1 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());

    Key partitionKey2 = mock(Key.class);
    Mutation mutation2 = createMutation(namespace1, table2, partitionKey2, Optional.empty());

    Key partitionKey3 = mock(Key.class);
    Mutation mutation3 = createMutation(namespace1, table1, partitionKey3, Optional.empty());

    Mutation mutation4 = createMutation(namespace2, table1, partitionKey1, Optional.empty());

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(Arrays.asList(mutation1, mutation2, mutation3, mutation4));

    // Assert
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).containsExactly(mutation1, mutation3);
    assertThat(result.get(1)).containsExactly(mutation2);
    assertThat(result.get(2)).containsExactly(mutation4);
  }

  @Test
  public void groupMutations_WithBatchSizeLimit_ShouldCreateMultipleBatches()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(2); // Max 2 mutations per batch
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create 5 mutations for the same table
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Key partitionKey = mock(Key.class);
      mutations.add(createMutation(namespace, table, partitionKey, Optional.empty()));
    }

    // Act
    List<List<Mutation>> result = grouper.groupMutations(mutations);

    // Assert
    assertThat(result).hasSize(3); // Should create 3 batches: 2+2+1
    assertThat(result.get(0).size()).isEqualTo(2);
    assertThat(result.get(1).size()).isEqualTo(2);
    assertThat(result.get(2).size()).isEqualTo(1);
  }

  @Test
  public void groupMutations_WithNamespaceAtomicity_ShouldGroupCorrectly()
      throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.NAMESPACE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo);
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo);

    // Create mutations for different namespaces
    Key partitionKey1 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());

    Key partitionKey2 = mock(Key.class);
    Mutation mutation2 = createMutation(namespace2, table2, partitionKey2, Optional.empty());

    Key partitionKey3 = mock(Key.class);
    Mutation mutation3 = createMutation(namespace1, table2, partitionKey3, Optional.empty());

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(Arrays.asList(mutation1, mutation2, mutation3));

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).containsExactly(mutation1, mutation3);
    assertThat(result.get(1)).containsExactly(mutation2);
  }

  @Test
  public void groupMutations_WithStorageAtomicity_ShouldGroupCorrectly() throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String namespace3 = "ns3";
    String namespace4 = "ns4";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo1 = mock(StorageInfo.class);
    when(storageInfo1.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.STORAGE);
    when(storageInfo1.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo1.getStorageName()).thenReturn("storage1");

    StorageInfo storageInfo2 = mock(StorageInfo.class);
    when(storageInfo2.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.STORAGE);
    when(storageInfo2.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo2.getStorageName()).thenReturn("storage2");

    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo1);
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo2);
    when(storageInfoProvider.getStorageInfo(namespace3)).thenReturn(storageInfo2);
    when(storageInfoProvider.getStorageInfo(namespace4)).thenReturn(storageInfo1);

    // Create mutations for different storages
    Key partitionKey1 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());

    Key partitionKey2 = mock(Key.class);
    Mutation mutation2 = createMutation(namespace2, table2, partitionKey2, Optional.empty());

    Key partitionKey3 = mock(Key.class);
    Mutation mutation3 = createMutation(namespace1, table2, partitionKey3, Optional.empty());

    Mutation mutation4 = createMutation(namespace3, table1, partitionKey1, Optional.empty());

    Mutation mutation5 = createMutation(namespace4, table2, partitionKey2, Optional.empty());

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(
            Arrays.asList(mutation1, mutation2, mutation3, mutation4, mutation5));

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).containsExactly(mutation1, mutation3, mutation5);
    assertThat(result.get(1)).containsExactly(mutation2, mutation4);
  }

  @Test
  public void groupMutations_WithStorageAtomicityAndBatchSizeLimit_ShouldGroupCorrectly()
      throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String namespace3 = "ns3";
    String namespace4 = "ns4";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo1 = mock(StorageInfo.class);
    when(storageInfo1.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.STORAGE);
    when(storageInfo1.getMaxAtomicMutationsCount()).thenReturn(2); // Max 2 mutations per batch
    when(storageInfo1.getStorageName()).thenReturn("storage1");

    StorageInfo storageInfo2 = mock(StorageInfo.class);
    when(storageInfo2.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.STORAGE);
    when(storageInfo2.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo2.getStorageName()).thenReturn("storage2");

    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo1);
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo2);
    when(storageInfoProvider.getStorageInfo(namespace3)).thenReturn(storageInfo2);
    when(storageInfoProvider.getStorageInfo(namespace4)).thenReturn(storageInfo1);

    // Create mutations for different storages
    Key partitionKey1 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());

    Key partitionKey2 = mock(Key.class);
    Mutation mutation2 = createMutation(namespace2, table2, partitionKey2, Optional.empty());

    Key partitionKey3 = mock(Key.class);
    Mutation mutation3 = createMutation(namespace1, table2, partitionKey3, Optional.empty());

    Mutation mutation4 = createMutation(namespace3, table1, partitionKey1, Optional.empty());

    Mutation mutation5 = createMutation(namespace4, table2, partitionKey2, Optional.empty());

    // Act
    List<List<Mutation>> result =
        grouper.groupMutations(
            Arrays.asList(mutation1, mutation2, mutation3, mutation4, mutation5));

    // Assert
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).containsExactly(mutation1, mutation3);
    assertThat(result.get(1)).containsExactly(mutation5);
    assertThat(result.get(2)).containsExactly(mutation2, mutation4);
  }

  @Test
  public void canBeGroupedTogether_WithEmptyCollection_ShouldReturnTrue()
      throws ExecutionException {
    // Act
    boolean result = grouper.canBeGroupedTogether(Collections.emptyList());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void canBeGroupedTogether_WithAllMutationsInSameGroupForRecordAtomicity_ShouldReturnTrue()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.RECORD);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create multiple mutations with same partition key, clustering key, table and namespace
    Key partitionKey1 = mock(Key.class);
    Key clusteringKey1 = mock(Key.class);
    Mutation mutation1 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));
    Mutation mutation2 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      canBeGroupedTogether_WithAllMutationsInSameGroupForPartitionAtomicity_ShouldReturnTrue()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.PARTITION);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create mutations with same partition key but different clustering keys
    Key partitionKey1 = mock(Key.class);
    Key clusteringKey1 = mock(Key.class);
    Key clusteringKey2 = mock(Key.class);
    Mutation mutation1 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey1));
    Mutation mutation2 =
        createMutation(namespace, table, partitionKey1, Optional.of(clusteringKey2));

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void canBeGroupedTogether_WithAllMutationsInSameGroupForTableAtomicity_ShouldReturnTrue()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create mutations with same table but different partition keys
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace, table, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace, table, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      canBeGroupedTogether_WithAllMutationsInSameGroupForNamespaceAtomicity_ShouldReturnTrue()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table1 = "table1";
    String table2 = "table2";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.NAMESPACE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create mutations with same namespace but different tables
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace, table1, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace, table2, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void canBeGroupedTogether_WithAllMutationsInSameGroupForStorageAtomicity_ShouldReturnTrue()
      throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.STORAGE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo);
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo);

    // Create mutations with different namespaces but same storage
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace2, table2, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void canBeGroupedTogether_WithMutationsInDifferentGroups_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo1 = mock(StorageInfo.class);
    when(storageInfo1.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo1.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo1.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace1)).thenReturn(storageInfo1);

    StorageInfo storageInfo2 = mock(StorageInfo.class);
    when(storageInfo2.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo2.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo2.getStorageName()).thenReturn("storage2");
    when(storageInfoProvider.getStorageInfo(namespace2)).thenReturn(storageInfo2);

    // Create mutations with different storage
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace1, table1, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace2, table2, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      canBeGroupedTogether_WithMutationsInDifferentTables_ShouldReturnFalseForTableAtomicity()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table1 = "table1";
    String table2 = "table2";

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create mutations with same namespace but different tables
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace, table1, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace, table2, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      canBeGroupedTogether_WithMutationsInDifferentPartitions_ShouldReturnFalseForPartitionAtomicity()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.PARTITION);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(100);
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create mutations with same table but different partition keys
    Key partitionKey1 = mock(Key.class);
    Key partitionKey2 = mock(Key.class);
    Mutation mutation1 = createMutation(namespace, table, partitionKey1, Optional.empty());
    Mutation mutation2 = createMutation(namespace, table, partitionKey2, Optional.empty());

    // Act
    boolean result = grouper.canBeGroupedTogether(Arrays.asList(mutation1, mutation2));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void canBeGroupedTogether_WithOverMaxCount_ShouldReturnFalse() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getMutationAtomicityUnit())
        .thenReturn(StorageInfo.MutationAtomicityUnit.TABLE);
    when(storageInfo.getMaxAtomicMutationsCount()).thenReturn(3); // Max 3 mutations allowed
    when(storageInfo.getStorageName()).thenReturn("storage1");
    when(storageInfoProvider.getStorageInfo(namespace)).thenReturn(storageInfo);

    // Create 4 mutations (one over max count)
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Key partitionKey = mock(Key.class);
      mutations.add(createMutation(namespace, table, partitionKey, Optional.empty()));
    }

    // Act
    boolean result = grouper.canBeGroupedTogether(mutations);

    // Assert
    assertThat(result).isFalse();
  }

  private Mutation createMutation(
      String namespace, String table, Key partitionKey, Optional<Key> clusteringKey) {
    Mutation mutation = mock(Put.class);
    when(mutation.forNamespace()).thenReturn(Optional.of(namespace));
    when(mutation.forTable()).thenReturn(Optional.of(table));
    when(mutation.getPartitionKey()).thenReturn(partitionKey);
    when(mutation.getClusteringKey()).thenReturn(clusteringKey);
    return mutation;
  }
}
