package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class MutationsGrouper {

  private final StorageInfoProvider storageInfoProvider;

  public MutationsGrouper(StorageInfoProvider storageInfoProvider) {
    this.storageInfoProvider = storageInfoProvider;
  }

  public List<List<Mutation>> groupMutations(Collection<Mutation> mutations)
      throws ExecutionException {
    // MutationGroup mutations by their storage info and atomicity unit
    Map<MutationGroup, List<List<Mutation>>> groupToBatches = new LinkedHashMap<>();

    for (Mutation mutation : mutations) {
      assert mutation.forNamespace().isPresent();
      StorageInfo storageInfo = storageInfoProvider.getStorageInfo(mutation.forNamespace().get());

      MutationGroup group = new MutationGroup(mutation, storageInfo);
      List<List<Mutation>> batches = groupToBatches.computeIfAbsent(group, g -> new ArrayList<>());
      int maxCount = group.storageInfo.getMaxAtomicMutationsCount();

      if (batches.isEmpty() || batches.get(batches.size() - 1).size() >= maxCount) {
        // If the last batch is full or there are no batches yet, create a new batch
        batches.add(new ArrayList<>());
      }

      batches.get(batches.size() - 1).add(mutation);
    }

    // Flatten the grouped mutations into a single list of batches
    return groupToBatches.values().stream().flatMap(List::stream).collect(Collectors.toList());
  }

  public boolean canBeGroupedTogether(Collection<Mutation> mutations) throws ExecutionException {
    if (mutations.size() <= 1) {
      return true;
    }

    Iterator<Mutation> iterator = mutations.iterator();
    Mutation firstMutation = iterator.next();
    assert firstMutation.forNamespace().isPresent();
    StorageInfo storageInfo =
        storageInfoProvider.getStorageInfo(firstMutation.forNamespace().get());
    MutationGroup firstGroup = new MutationGroup(firstMutation, storageInfo);

    int maxCount = firstGroup.storageInfo.getMaxAtomicMutationsCount();
    int mutationCount = 1;

    while (iterator.hasNext()) {
      Mutation otherMutation = iterator.next();
      assert otherMutation.forNamespace().isPresent();
      StorageInfo otherStorageInfo =
          storageInfoProvider.getStorageInfo(otherMutation.forNamespace().get());
      MutationGroup otherGroup = new MutationGroup(otherMutation, otherStorageInfo);

      if (!firstGroup.equals(otherGroup)) {
        return false; // Found a mutation that does not belong to the first group
      }

      mutationCount++;
      if (mutationCount > maxCount) {
        return false; // Exceeds the maximum allowed count for this group
      }
    }

    return true; // All mutations belong to the same group and within the count limit
  }

  private static class MutationGroup {
    public final StorageInfo storageInfo;
    @Nullable public final String namespace;
    @Nullable public final String table;
    @Nullable public final Key partitionKey;
    @Nullable public final Optional<Key> clusteringKey;

    private MutationGroup(Mutation mutation, StorageInfo storageInfo) {
      assert mutation.forNamespace().isPresent() && mutation.forTable().isPresent();

      switch (storageInfo.getMutationAtomicityUnit()) {
        case RECORD:
          this.clusteringKey = mutation.getClusteringKey();
          this.partitionKey = mutation.getPartitionKey();
          this.table = mutation.forTable().get();
          this.namespace = mutation.forNamespace().get();
          this.storageInfo = storageInfo;
          break;
        case PARTITION:
          this.clusteringKey = null;
          this.partitionKey = mutation.getPartitionKey();
          this.table = mutation.forTable().get();
          this.namespace = mutation.forNamespace().get();
          this.storageInfo = storageInfo;
          break;
        case TABLE:
          this.clusteringKey = null;
          this.partitionKey = null;
          this.table = mutation.forTable().get();
          this.namespace = mutation.forNamespace().get();
          this.storageInfo = storageInfo;
          break;
        case NAMESPACE:
          this.clusteringKey = null;
          this.partitionKey = null;
          this.table = null;
          this.namespace = mutation.forNamespace().get();
          this.storageInfo = storageInfo;
          break;
        case STORAGE:
          this.clusteringKey = null;
          this.partitionKey = null;
          this.table = null;
          this.namespace = null;
          this.storageInfo = storageInfo;
          break;
        default:
          throw new AssertionError(
              "Unknown mutation atomicity unit: " + storageInfo.getMutationAtomicityUnit());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MutationGroup)) {
        return false;
      }
      MutationGroup that = (MutationGroup) o;
      return Objects.equals(storageInfo.getStorageName(), that.storageInfo.getStorageName())
          && Objects.equals(namespace, that.namespace)
          && Objects.equals(table, that.table)
          && Objects.equals(partitionKey, that.partitionKey)
          && Objects.equals(clusteringKey, that.clusteringKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          storageInfo.getStorageName(), namespace, table, partitionKey, clusteringKey);
    }
  }
}
