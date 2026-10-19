package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ObjectStorageTestUtils {

  private ObjectStorageTestUtils() {}

  /**
   * Puts the records with a single mutation per partition. Since Object Storage stores all the
   * records in a partition as a single object, this avoids writing the same object repeatedly.
   */
  public static void putRecordsPerPartition(DistributedStorage storage, List<Put> puts)
      throws ExecutionException {
    Map<List<Object>, List<Put>> putsPerPartition = new LinkedHashMap<>();
    for (Put put : puts) {
      putsPerPartition
          .computeIfAbsent(
              Arrays.asList(put.forNamespace(), put.forTable(), put.getPartitionKey()),
              k -> new ArrayList<>())
          .add(put);
    }
    for (List<Put> partitionPuts : putsPerPartition.values()) {
      storage.mutate(partitionPuts);
    }
  }
}
