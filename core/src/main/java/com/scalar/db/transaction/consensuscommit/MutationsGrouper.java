package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    List<MutationGroup> groups = new ArrayList<>();
    Map<MutationGroup, List<List<Mutation>>> groupToBatches = new LinkedHashMap<>();

    for (Mutation mutation : mutations) {
      assert mutation.forNamespace().isPresent();
      StorageInfo storageInfo = storageInfoProvider.getStorageInfo(mutation.forNamespace().get());

      MutationGroup matchedGroup = null;
      for (MutationGroup group : groups) {
        if (group.isSameGroup(mutation, storageInfo)) {
          matchedGroup = group;
          break;
        }
      }
      if (matchedGroup == null) {
        // If no matching group is found, create a new one
        matchedGroup = new MutationGroup(mutation, storageInfo);
        groups.add(matchedGroup);
      }

      List<List<Mutation>> batches =
          groupToBatches.computeIfAbsent(matchedGroup, g -> new ArrayList<>());
      int maxCount = matchedGroup.storageInfo.getMaxAtomicMutationsCount();

      if (batches.isEmpty() || batches.get(batches.size() - 1).size() >= maxCount) {
        // If the last batch is full or there are no batches yet, create a new batch
        batches.add(new ArrayList<>());
      }

      batches.get(batches.size() - 1).add(mutation);
    }

    // Flatten the grouped mutations into a single list of batches
    return groupToBatches.values().stream().flatMap(List::stream).collect(Collectors.toList());
  }

  private static class MutationGroup {
    public final StorageInfo storageInfo;
    @Nullable public final String namespace;
    @Nullable public final String table;
    @Nullable public final Key partitionKey;
    @Nullable public final Optional<Key> clusteringKey;

    public MutationGroup(Mutation mutation, StorageInfo storageInfo) {
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

    boolean isSameGroup(Mutation otherMutation, StorageInfo otherStorageInfo) {
      assert otherMutation.forNamespace().isPresent() && otherMutation.forTable().isPresent();

      switch (storageInfo.getMutationAtomicityUnit()) {
        case RECORD:
          if (!otherMutation.getClusteringKey().equals(this.clusteringKey)) {
            return false;
          }
          // Fall through
        case PARTITION:
          if (!otherMutation.getPartitionKey().equals(this.partitionKey)) {
            return false;
          }
          // Fall through
        case TABLE:
          if (!otherMutation.forTable().get().equals(this.table)) {
            return false;
          }
          // Fall through
        case NAMESPACE:
          if (!otherMutation.forNamespace().get().equals(this.namespace)) {
            return false;
          }
          // Fall through
        case STORAGE:
          if (!otherStorageInfo.getStorageName().equals(this.storageInfo.getStorageName())) {
            return false;
          }
          break;
        default:
          throw new AssertionError(
              "Unknown mutation atomicity unit: " + storageInfo.getMutationAtomicityUnit());
      }

      return true;
    }
  }
}
