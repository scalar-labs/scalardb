package com.scalar.db.storage.objectstorage;

import com.google.cloud.storage.StorageException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.CoordinatorStateAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class ObjectStorageTestUtils {

  // Object Storage serializes numbers as JSON numbers, which can only guarantee integer precision
  // up to 2^53. BigInt values must be within this range.
  public static final long BIGINT_MAX_VALUE = 9007199254740992L; // 2^53
  public static final long BIGINT_MIN_VALUE = -9007199254740992L; // -2^53

  private static final int MAX_RETRY_COUNT_ON_TRANSIENT_ERROR = 3;

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

  /**
   * Executes the admin operation, retrying it on the errors that the Cloud Storage client regards
   * as retryable, such as the rate limit errors on deleting objects that have just been written.
   */
  public static void executeWithRetryOnTransientError(AdminOperation operation)
      throws ExecutionException {
    for (int retryCount = 0; ; retryCount++) {
      try {
        operation.execute();
        return;
      } catch (ExecutionException e) {
        if (retryCount >= MAX_RETRY_COUNT_ON_TRANSIENT_ERROR || !isTransientError(e)) {
          throw e;
        }
        // Wait for the rate limit of one write per second to be reset
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Drops the coordinator table and its namespace separately with retries, since dropping them with
   * {@code dropCoordinatorTables()} cannot be retried once the table has been dropped.
   */
  public static void dropCoordinatorTablesWithRetryOnTransientError(Properties properties)
      throws ExecutionException {
    String coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(properties))
            .getCoordinatorNamespace()
            .orElse(CoordinatorStateAccessor.NAMESPACE);
    try (DistributedStorageAdmin admin = StorageFactory.create(properties).getStorageAdmin()) {
      executeWithRetryOnTransientError(
          () -> admin.dropTable(coordinatorNamespace, CoordinatorStateAccessor.TABLE));
      executeWithRetryOnTransientError(() -> admin.dropNamespace(coordinatorNamespace));
    }
  }

  private static boolean isTransientError(Throwable throwable) {
    return Throwables.getCausalChain(throwable).stream()
        .anyMatch(t -> t instanceof StorageException && ((StorageException) t).isRetryable());
  }

  @FunctionalInterface
  public interface AdminOperation {
    void execute() throws ExecutionException;
  }
}
