package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected int getLargeDataSizeInBytes() {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      // For Oracle, the max data size for BLOB is 2000 bytes
      return 2000;
    } else {
      return super.getLargeDataSizeInBytes();
    }
  }

  @Test
  public void get_InStreamingMode_ShouldRetrieveSingleResult() throws ExecutionException {
    if (!JdbcTestUtils.isMysql(rdbEngine)) {
      // MySQL is the only RDB engine that supports streaming mode
      return;
    }

    try (DistributedStorage storage = getStorageInStreamingMode()) {
      // Arrange
      int pKey = 0;
      int cKey = 1;
      int value = 2;

      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, pKey))
              .clusteringKey(Key.ofInt(COL_NAME4, cKey))
              .intValue(COL_NAME3, value)
              .build());

      // Act
      Optional<Result> result =
          storage.get(
              Get.newBuilder()
                  .namespace(namespace)
                  .table(TABLE)
                  .partitionKey(Key.ofInt(COL_NAME1, pKey))
                  .clusteringKey(Key.ofInt(COL_NAME4, cKey))
                  .build());

      // Assert
      assertThat(result.isPresent()).isTrue();
      assertThat(result.get().getInt(COL_NAME1)).isEqualTo(pKey);
      assertThat(result.get().getInt(COL_NAME4)).isEqualTo(cKey);
      assertThat(result.get().getInt(COL_NAME3)).isEqualTo(value);
    }
  }

  @Test
  public void scan_InStreamingMode_ShouldRetrieveResults() throws IOException, ExecutionException {
    if (!JdbcTestUtils.isMysql(rdbEngine)) {
      // MySQL is the only RDB engine that supports streaming mode
      return;
    }

    try (DistributedStorage storage = getStorageInStreamingMode()) {
      // Arrange
      int pKey = 0;

      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, pKey))
              .clusteringKey(Key.ofInt(COL_NAME4, 0))
              .intValue(COL_NAME3, 1)
              .build());
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, pKey))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
              .intValue(COL_NAME3, 2)
              .build());
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, pKey))
              .clusteringKey(Key.ofInt(COL_NAME4, 2))
              .intValue(COL_NAME3, 3)
              .build());

      // Act
      Scanner scanner =
          storage.scan(
              Scan.newBuilder()
                  .namespace(namespace)
                  .table(TABLE)
                  .partitionKey(Key.ofInt(COL_NAME1, pKey))
                  .build());
      List<Result> results = scanner.all();
      scanner.close();

      // Assert
      assertThat(results).hasSize(3);
      assertThat(results.get(0).getInt(COL_NAME1)).isEqualTo(pKey);
      assertThat(results.get(0).getInt(COL_NAME4)).isEqualTo(0);
      assertThat(results.get(0).getInt(COL_NAME3)).isEqualTo(1);

      assertThat(results.get(1).getInt(COL_NAME1)).isEqualTo(pKey);
      assertThat(results.get(1).getInt(COL_NAME4)).isEqualTo(1);
      assertThat(results.get(1).getInt(COL_NAME3)).isEqualTo(2);

      assertThat(results.get(2).getInt(COL_NAME1)).isEqualTo(pKey);
      assertThat(results.get(2).getInt(COL_NAME4)).isEqualTo(2);
      assertThat(results.get(2).getInt(COL_NAME3)).isEqualTo(3);
    }
  }

  private DistributedStorage getStorageInStreamingMode() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    properties.setProperty(DatabaseConfig.SCAN_FETCH_SIZE, Integer.toString(Integer.MIN_VALUE));
    return StorageFactory.create(properties).getStorage();
  }
}
