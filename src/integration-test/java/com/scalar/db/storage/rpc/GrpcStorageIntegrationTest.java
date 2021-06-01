package com.scalar.db.storage.rpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.server.ScalarDbServer;
import com.scalar.db.storage.IntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GrpcStorageIntegrationTest extends IntegrationTestBase {

  private static final String CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "mysql";

  private static TestEnv testEnv;
  private static ScalarDbServer server;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  @Test
  public void scan_ScanLargeData_ShouldRetrieveExpectedValues() throws Exception {
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
  public void scan_ScanLargeDataWithOrdering_ShouldRetrieveExpectedValues() throws Exception {
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
      assertThat(results.get(i).getPartitionKey().get().get().get(0).getAsInt()).isEqualTo(1);
      assertThat(results.get(i).getClusteringKey().get().get().get(0).getAsInt()).isEqualTo(i);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv(CONTACT_POINT, USERNAME, PASSWORD, Optional.empty());
    testEnv.register(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addSecondaryIndex(COL_NAME3)
            .build());
    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    server = new ScalarDbServer(testEnv.getJdbcDatabaseConfig().getProperties());
    server.start();

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    properties.setProperty(DatabaseConfig.CONTACT_PORT, "60051");
    storage = new GrpcStorage(new DatabaseConfig(properties));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    storage.close();
    server.shutdown();
    server.blockUntilShutdown();
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
