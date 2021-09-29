package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamoMultipleClusteringKeysIntegrationTest {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String TABLE = "test_table";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final String COL_NAME6 = "c6";
  protected static final String COL_NAME7 = "c7";
  protected static final String COL_NAME8 = "c8";
  protected static final String COL_NAME9 = "c9";
  protected static final String COL_NAME10 = "c10";

  private static Optional<String> namespacePrefix;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage distributedStorage;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    String endpointOverride =
        System.getProperty("scalardb.dynamo.endpoint_override", "http://localhost:8000");
    String region = System.getProperty("scalardb.dynamo.region", "us-west-2");
    String accessKeyId = System.getProperty("scalardb.dynamo.access_key_id", "fakeMyKeyId");
    String secretAccessKey =
        System.getProperty("scalardb.dynamo.secret_access_key", "fakeSecretAccessKey");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));

    StorageFactory storageFactory = new StorageFactory(new DatabaseConfig(props));
    distributedStorage = storageFactory.getStorage();

    admin = new DynamoAdmin(new DynamoConfig(props));
    admin.createNamespace(NAMESPACE, Collections.emptyMap());
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.DOUBLE)
            .addColumn(COL_NAME6, DataType.TEXT)
            .addColumn(COL_NAME7, DataType.INT)
            .addColumn(COL_NAME8, DataType.FLOAT)
            .addColumn(COL_NAME9, DataType.BLOB)
            .addColumn(COL_NAME10, DataType.INT)
            .addPartitionKey(COL_NAME1)
            .addPartitionKey(COL_NAME2)
            .addClusteringKey(COL_NAME3)
            .addClusteringKey(COL_NAME4)
            .addClusteringKey(COL_NAME5, Order.DESC)
            .addClusteringKey(COL_NAME6)
            .addClusteringKey(COL_NAME7)
            .addClusteringKey(COL_NAME8)
            .addClusteringKey(COL_NAME9)
            .build(),
        ImmutableMap.of("no-scaling", "true", "no-backup", "true"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE);
    admin.dropNamespace(NAMESPACE);
    admin.close();
  }

  @Before
  public void setUp() {}

  @Test
  public void scan_WithClusteringKeyRangeOfDoubleValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Double> doubleList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      double col5Val = randomGenerator.nextDouble();
      doubleList.add(col5Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, col5Val),
                      new TextValue(COL_NAME6, "col6val" + i),
                      new IntValue(COL_NAME7, 7 + i),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    doubleList.sort(Double::compareTo);
    Collections.reverse(doubleList);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 50; i++) {
      expectedValues.add(new DoubleValue(COL_NAME5, doubleList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, doubleList.get(49))),
                true)
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, doubleList.get(25))),
                true)
            .withOrdering(new Scan.Ordering(COL_NAME3, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(COL_NAME4, Order.ASC))
            .withOrdering(new Scan.Ordering(COL_NAME5, Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME5, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyRangeOfStringValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    List<String> stringList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String col6Val = RandomStringUtils.randomAlphanumeric(20);
      stringList.add(col6Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, col6Val),
                      new IntValue(COL_NAME7, 7 + i),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    stringList.sort(String::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 50; i++) {
      expectedValues.add(new TextValue(COL_NAME6, stringList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, stringList.get(25))),
                true)
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, stringList.get(49))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME6, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyRangeOfIntegerValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Integer> integerList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int col7Val = randomGenerator.nextInt();
      integerList.add(col7Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, col7Val),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    integerList.sort(Integer::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 50; i++) {
      expectedValues.add(new IntValue(COL_NAME7, integerList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(25))),
                true)
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(49))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME7, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyRangeOfFloatValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Float> floatList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      float col8Val = randomGenerator.nextFloat();
      floatList.add(col8Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, 7),
                      new FloatValue(COL_NAME8, col8Val),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    floatList.sort(Float::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 50; i++) {
      expectedValues.add(new FloatValue(COL_NAME8, floatList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, 7),
                    new FloatValue(COL_NAME8, floatList.get(25))),
                true)
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, 7),
                    new FloatValue(COL_NAME8, floatList.get(49))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME8, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyRangeOfBlobValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<byte[]> bytesList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] col9Val = new byte[20];
      randomGenerator.nextBytes(col9Val);
      bytesList.add(col9Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, 7),
                      new FloatValue(COL_NAME8, 8),
                      new BlobValue(COL_NAME9, col9Val)))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    bytesList.sort(UnsignedBytes.lexicographicalComparator());

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 50; i++) {
      expectedValues.add(new BlobValue(COL_NAME9, bytesList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, 7),
                    new FloatValue(COL_NAME8, 8),
                    new BlobValue(COL_NAME9, bytesList.get(25))),
                true)
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, 7),
                    new FloatValue(COL_NAME8, 8),
                    new BlobValue(COL_NAME9, bytesList.get(49))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME9, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyStartInclusiveRangeOfIntegerValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Integer> integerList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int col7Val = randomGenerator.nextInt();
      integerList.add(col7Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, col7Val),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    integerList.sort(Integer::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 25; i < 100; i++) {
      expectedValues.add(new IntValue(COL_NAME7, integerList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(25))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME7, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyStartExclusiveRangeOfIntegerValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Integer> integerList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int col7Val = randomGenerator.nextInt();
      integerList.add(col7Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, col7Val),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    integerList.sort(Integer::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 26; i < 100; i++) {
      expectedValues.add(new IntValue(COL_NAME7, integerList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withStart(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(25))),
                false)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME7, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfIntegerValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Integer> integerList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int col7Val = randomGenerator.nextInt();
      integerList.add(col7Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, col7Val),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    integerList.sort(Integer::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      expectedValues.add(new IntValue(COL_NAME7, integerList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(49))),
                true)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME7, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfIntegerValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    // Arrange
    Random randomGenerator = new Random();
    List<Integer> integerList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int col7Val = randomGenerator.nextInt();
      integerList.add(col7Val);
      Put put =
          new Put(
                  new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")),
                  new Key(
                      new TextValue(COL_NAME3, "col3val"),
                      new IntValue(COL_NAME4, 4),
                      new DoubleValue(COL_NAME5, 5),
                      new TextValue(COL_NAME6, "col6val"),
                      new IntValue(COL_NAME7, col7Val),
                      new FloatValue(COL_NAME8, 8 + i),
                      new BlobValue(COL_NAME9, new byte[10])))
              .withValue(new IntValue(COL_NAME10, 10 + i))
              .forNamespace(NAMESPACE)
              .forTable(TABLE);
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    integerList.sort(Integer::compareTo);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 0; i < 49; i++) {
      expectedValues.add(new IntValue(COL_NAME7, integerList.get(i)));
    }

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 1), new TextValue(COL_NAME2, "col2val")))
            .withEnd(
                new Key(
                    new TextValue(COL_NAME3, "col3val"),
                    new IntValue(COL_NAME4, 4),
                    new DoubleValue(COL_NAME5, 5),
                    new TextValue(COL_NAME6, "col6val"),
                    new IntValue(COL_NAME7, integerList.get(49))),
                false)
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME7, expectedValues);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  private void assertScanResultWithOrdering(
      List<Result> actual, String checkedColumn, List<Value<?>> expectedValues) {
    assertThat(actual.size()).isEqualTo(expectedValues.size());

    for (int i = 0; i < actual.size(); i++) {
      Value<?> expectedValue = expectedValues.get(i);
      Result actualResult = actual.get(i);
      Value<?> actualValue = actualResult.getValue(checkedColumn).get();
      assertThat(actualValue).isEqualTo(expectedValue);
    }
  }
}
