package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

@SuppressWarnings("unchecked")
public abstract class MultipleClusteringKeysIntegrationTestBase {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String TABLE_BASE_NAME = "test_table_mul_key_";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";

  protected static final ImmutableList<DataType> CLUSTERING_KEY_TYPE_LIST =
      ImmutableList.of(
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.BLOB,
          DataType.FLOAT,
          DataType.INT,
          DataType.TEXT);

  protected static final Random RANDOM_GENERATOR = new Random();

  protected static Optional<String> namespacePrefix;
  protected static DistributedStorageAdmin admin;
  protected static DistributedStorage distributedStorage;

  @Test
  public void scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    RANDOM_GENERATOR.setSeed(777);

    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        List<Value> valueList = new ArrayList<>();
        prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

        List<Value<?>> expectedValues = new ArrayList<>();
        for (int i = 25; i < 50; i++) {
          expectedValues.add(valueList.get(i));
        }

        Scan scan =
            new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
                .withStart(
                    new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(25)), true)
                .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(49)), true)
                .forNamespace(NAMESPACE)
                .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

        // Act
        List<Result> scanRet = distributedStorage.scan(scan).all();

        // Assert
        assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
      }
    }

    truncateTestTables();
  }

  @Test
  public void scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    RANDOM_GENERATOR.setSeed(778);

    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        List<Value> valueList = new ArrayList<>();
        prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

        List<Value<?>> expectedValues = new ArrayList<>();
        for (int i = 25; i < 100; i++) {
          expectedValues.add(valueList.get(i));
        }

        Scan scan =
            new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
                .withStart(
                    new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(25)), true)
                .forNamespace(NAMESPACE)
                .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

        // Act
        List<Result> scanRet = distributedStorage.scan(scan).all();

        // Assert
        assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
      }
    }

    truncateTestTables();
  }

  @Test
  public void scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    RANDOM_GENERATOR.setSeed(779);

    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        List<Value> valueList = new ArrayList<>();
        prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

        List<Value<?>> expectedValues = new ArrayList<>();
        for (int i = 26; i < 100; i++) {
          expectedValues.add(valueList.get(i));
        }

        Scan scan =
            new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
                .withStart(
                    new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(25)), false)
                .forNamespace(NAMESPACE)
                .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

        // Act
        List<Result> scanRet = distributedStorage.scan(scan).all();

        // Assert
        assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
      }
    }

    truncateTestTables();
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    RANDOM_GENERATOR.setSeed(780);

    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        List<Value> valueList = new ArrayList<>();
        prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

        List<Value<?>> expectedValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
          expectedValues.add(valueList.get(i));
        }

        Scan scan =
            new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
                .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(49)), true)
                .forNamespace(NAMESPACE)
                .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

        // Act
        List<Result> scanRet = distributedStorage.scan(scan).all();

        // Assert
        assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
      }
    }

    truncateTestTables();
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult()
      throws ExecutionException {
    RANDOM_GENERATOR.setSeed(781);

    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        List<Value> valueList = new ArrayList<>();
        prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

        List<Value<?>> expectedValues = new ArrayList<>();
        for (int i = 0; i < 49; i++) {
          expectedValues.add(valueList.get(i));
        }

        Scan scan =
            new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
                .withEnd(
                    new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(49)), false)
                .forNamespace(NAMESPACE)
                .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

        // Act
        List<Result> scanRet = distributedStorage.scan(scan).all();

        // Assert
        assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
      }
    }

    truncateTestTables();
  }

  protected void prepareRecords(
      List<Value> valueList, DataType cKeyTypeBefore, DataType cKeyTypeAfter)
      throws ExecutionException {
    for (int i = 0; i < 100; i++) {

      Value cKeyValueAfter = getRandomValue(COL_NAME3, cKeyTypeAfter);

      valueList.add(cKeyValueAfter);
      Put put =
          new Put(
                  new Key(getFixedValue(COL_NAME1, DataType.INT)),
                  new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), cKeyValueAfter))
              .withValue(getRandomValue(COL_NAME4, DataType.INT))
              .withValue(getRandomValue(COL_NAME5, DataType.TEXT))
              .forNamespace(NAMESPACE)
              .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));
      try {
        distributedStorage.put(put);
      } catch (ExecutionException e) {
        throw new ExecutionException("put data to database failed");
      }
    }

    valueList.sort(Value::compareTo);
  }

  protected static void createTestTables(Map<String, String> options) throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        createTable(cKeyTypeBefore, cKeyTypeAfter, options);
      }
    }
  }

  protected static void truncateTestTables() throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        admin.truncateTable(NAMESPACE, getTableName(cKeyTypeBefore, cKeyTypeAfter));
      }
    }
  }

  protected static void deleteTestTables() throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        admin.dropTable(NAMESPACE, getTableName(cKeyTypeBefore, cKeyTypeAfter));
      }
    }
  }

  protected static void createTable(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter, Map<String, String> options)
      throws ExecutionException {
    admin.createNamespace(NAMESPACE, true, options);
    admin.createTable(
        NAMESPACE,
        getTableName(cKeyTypeBefore, cKeyTypeAfter),
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, cKeyTypeBefore)
            .addColumn(COL_NAME3, cKeyTypeAfter)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.TEXT)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME2)
            .addClusteringKey(COL_NAME3)
            .build(),
        options);
  }

  protected static String getTableName(DataType cKeyTypeBefore, DataType cKeyTypeAfter) {
    return TABLE_BASE_NAME + cKeyTypeBefore + "_" + cKeyTypeAfter;
  }

  protected Value<?> getRandomValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, RANDOM_GENERATOR.nextLong());
      case INT:
        return new IntValue(columnName, RANDOM_GENERATOR.nextInt());
      case FLOAT:
        return new FloatValue(columnName, RANDOM_GENERATOR.nextFloat());
      case DOUBLE:
        return new DoubleValue(columnName, RANDOM_GENERATOR.nextDouble());
      case BLOB:
        byte[] bytes = new byte[20];
        RANDOM_GENERATOR.nextBytes(bytes);
        return new BlobValue(columnName, bytes);
      case TEXT:
        return new TextValue(
            columnName, RandomStringUtils.random(20, 0, 0, true, true, null, RANDOM_GENERATOR));
      default:
        throw new RuntimeException("Unsupported data type for random generating");
    }
  }

  protected Value<?> getFixedValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, 1);
      case INT:
        return new IntValue(columnName, 1);
      case FLOAT:
        return new FloatValue(columnName, 1);
      case DOUBLE:
        return new DoubleValue(columnName, 1);
      case BLOB:
        return new BlobValue(columnName, new byte[] {1, 1, 1, 1, 1});
      case TEXT:
        return new TextValue(columnName, "fixed_text");
      default:
        throw new RuntimeException("Unsupported data type");
    }
  }

  protected void assertScanResultWithOrdering(
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
