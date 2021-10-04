package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Test;

public class DistributedStorageServiceIntegrationTest extends StorageIntegrationTestBase {

  private static ScalarDbServer server;

  @Override
  protected void initialize() throws IOException {
    ServerConfig config = ServerEnv.getServerConfig();
    if (config != null) {
      server = new ScalarDbServer(config);
      server.start();
    }
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return ServerEnv.getGrpcConfig();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    StorageIntegrationTestBase.tearDownAfterClass();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
  }

  @Test
  public void scan_ScanLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = Key.newBuilder().addInt(COL_NAME1, 1).build();
    List<Integer> expectedValues = new ArrayList<>();
    for (int i = 0; i < 345; i++) {
      Key clusteringKey = Key.newBuilder().addInt(COL_NAME4, i).build();
      storage.put(new Put(partitionKey, clusteringKey));
      expectedValues.add(i);
    }
    Scan scan = new Scan(partitionKey);

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(345);
    assertScanResultWithoutOrdering(results, 1, COL_NAME4, expectedValues);
  }

  @Test
  public void scan_ScanLargeDataWithOrdering_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = Key.newBuilder().addInt(COL_NAME1, 1).build();
    for (int i = 0; i < 345; i++) {
      Key clusteringKey = Key.newBuilder().addInt(COL_NAME4, i).build();
      storage.put(new Put(partitionKey, clusteringKey));
    }
    Scan scan = new Scan(partitionKey).withOrdering(new Ordering(COL_NAME4, Order.ASC));

    // Act
    List<Result> results = new ArrayList<>();
    try (Scanner scanner = storage.scan(scan)) {
      Iterator<Result> iterator = scanner.iterator();
      for (int i = 0; i < 234; i++) {
        results.add(iterator.next());
      }
    }

    // Assert
    assertThat(results.size()).isEqualTo(234);
    for (int i = 0; i < 234; i++) {
      assertThat(results.get(i).getPartitionKey().isPresent()).isTrue();
      assertThat(results.get(i).getClusteringKey().isPresent()).isTrue();

      assertThat(results.get(i).getPartitionKey().get().get().get(0).getAsInt()).isEqualTo(1);
      assertThat(results.get(i).getClusteringKey().get().get().get(0).getAsInt()).isEqualTo(i);
    }
  }
}
