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

  private static final int MAX_RETRY_COUNT_ON_RATE_LIMIT = 3;

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
   * Executes the admin operation, retrying it when it fails because of the rate limit of Cloud
   * Storage. Cloud Storage limits the writes, including the deletions, to the same object to one
   * per second, so an operation that deletes objects, such as truncating or dropping a table, can
   * fail when the objects have been written just before. Since such a failure is reported not only
   * as 429 Too Many Requests but also as a server error, such as 503 Service Unavailable, all the
   * errors that the Cloud Storage client regards as retryable are retried. The operation is
   * expected to be safe to retry.
   *
   * @param operation the admin operation to execute
   * @throws ExecutionException if the operation fails for a reason other than the rate limit, or
   *     keeps failing after retrying
   */
  public static void executeWithRetryOnRateLimit(AdminOperation operation)
      throws ExecutionException {
    for (int retryCount = 0; ; retryCount++) {
      try {
        operation.execute();
        return;
      } catch (ExecutionException e) {
        if (retryCount >= MAX_RETRY_COUNT_ON_RATE_LIMIT || !isRateLimitExceeded(e)) {
          throw e;
        }
        // Wait for the rate limit of one write per second to be reset
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Drops the coordinator table and its namespace of Consensus Commit, retrying each of them when
   * it fails because of the rate limit of Cloud Storage. Dropping them with {@link
   * com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin#dropCoordinatorTables()} cannot
   * be retried as a whole, since a retry after the table has been dropped fails because the
   * coordinator tables no longer exist.
   *
   * @param properties the properties of the test, which specify the coordinator namespace
   * @throws ExecutionException if dropping the table or the namespace fails for a reason other than
   *     the rate limit, or keeps failing after retrying
   */
  public static void dropCoordinatorTablesWithRetryOnRateLimit(Properties properties)
      throws ExecutionException {
    String coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(properties))
            .getCoordinatorNamespace()
            .orElse(CoordinatorStateAccessor.NAMESPACE);
    try (DistributedStorageAdmin admin = StorageFactory.create(properties).getStorageAdmin()) {
      executeWithRetryOnRateLimit(
          () -> admin.dropTable(coordinatorNamespace, CoordinatorStateAccessor.TABLE));
      executeWithRetryOnRateLimit(() -> admin.dropNamespace(coordinatorNamespace));
    }
  }

  private static boolean isRateLimitExceeded(Throwable throwable) {
    return Throwables.getCausalChain(throwable).stream()
        .anyMatch(t -> t instanceof StorageException && ((StorageException) t).isRetryable());
  }

  @FunctionalInterface
  public interface AdminOperation {
    void execute() throws ExecutionException;
  }
}
