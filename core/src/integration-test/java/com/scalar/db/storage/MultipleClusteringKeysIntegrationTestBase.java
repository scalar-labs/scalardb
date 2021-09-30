package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

@SuppressWarnings("unchecked")
@SuppressFBWarnings("MS_CANNOT_BE_FINAL")
public abstract class MultipleClusteringKeysIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "integration_testing_";
  protected static final String TABLE_BASE_NAME = "test_table_mul_key_";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";

  protected static final ImmutableList<DataType> CLUSTERING_KEY_TYPE_LIST =
      ImmutableList.of(
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.BLOB,
          DataType.DOUBLE,
          DataType.INT,
          DataType.TEXT);

  protected static final Random RANDOM_GENERATOR = new Random();

  protected static Optional<String> namespacePrefix;
  protected static DistributedStorageAdmin admin;
  protected static DistributedStorage distributedStorage;

  @Test
  public void scan_WithClusteringKeyRangeOfValuesDoubleAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.DOUBLE);
    }
  }

  @Test
  public void scan_WithClusteringKeyRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.FLOAT);
    }
  }

  @Test
  public void scan_WithClusteringKeyRangeOfValuesIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(cKeyTypeBefore, DataType.INT);
    }
  }

  @Test
  public void scan_WithClusteringKeyRangeOfValuesBigIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BIGINT);
    }
  }

  @Test
  public void scan_WithClusteringKeyRangeOfValuesBlobAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(cKeyTypeBefore, DataType.BLOB);
    }
  }

  @Test
  public void scan_WithClusteringKeyRangeOfValuesTextAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(cKeyTypeBefore, DataType.TEXT);
    }
  }

  public void scan_WithClusteringKeyRangeOfValues_ShouldReturnProperlyResult(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter) throws ExecutionException {
    RANDOM_GENERATOR.setSeed(777);
    List<Value> valueList = Collections.synchronizedList(new ArrayList<>());
    prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 5; i < 15; i++) {
      expectedValues.add(valueList.get(i));
    }

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(5)), true)
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(14)), true)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC))
            .withOrdering(new Ordering(COL_NAME3, Order.ASC))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
  }

  @Test
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesDoubleAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.DOUBLE);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.FLOAT);
    }
  }

  @Test
  public void scan_WithClusteringKeyStartInclusiveRangeOfValuesIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.INT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesBigIntAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BIGINT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BLOB);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesTextAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.TEXT);
    }
  }

  public void scan_WithClusteringKeyStartInclusiveRangeOfValues_ShouldReturnProperlyResult(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter) throws ExecutionException {
    //    RANDOM_GENERATOR.setSeed(778);
    List<Value> valueList = Collections.synchronizedList(new ArrayList<>());
    prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 5; i < 20; i++) {
      expectedValues.add(valueList.get(i));
    }

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(5)), true)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC))
            .withOrdering(new Ordering(COL_NAME3, Order.ASC))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
  }

  @Test
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesDoubleAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.DOUBLE);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.FLOAT);
    }
  }

  @Test
  public void scan_WithClusteringKeyStartExclusiveRangeOfValuesIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.INT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesBigIntAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BIGINT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BLOB);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesTextAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.TEXT);
    }
  }

  public void scan_WithClusteringKeyStartExclusiveRangeOfValues_ShouldReturnProperlyResult(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter) throws ExecutionException {
    RANDOM_GENERATOR.setSeed(779);

    List<Value> valueList = Collections.synchronizedList(new ArrayList<>());
    prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 6; i < 20; i++) {
      expectedValues.add(valueList.get(i));
    }

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(5)), false)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC))
            .withOrdering(new Ordering(COL_NAME3, Order.ASC))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
  }

  @Test
  public void
      scan_WithClusteringKeyEndInclusiveRangeOfValuesDoubleAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.DOUBLE);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.FLOAT);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfValuesIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.INT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyEndInclusiveRangeOfValuesBigIntAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BIGINT);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BLOB);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRangeOfValuesTextAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.TEXT);
    }
  }

  public void scan_WithClusteringKeyEndInclusiveRangeOfValues_ShouldReturnProperlyResult(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter) throws ExecutionException {
    RANDOM_GENERATOR.setSeed(780);

    List<Value> valueList = Collections.synchronizedList(new ArrayList<>());
    prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expectedValues.add(valueList.get(i));
    }

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(9)), true)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC))
            .withOrdering(new Ordering(COL_NAME3, Order.ASC))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
  }

  @Test
  public void
      scan_WithClusteringKeyEndExclusiveRangeOfValuesDoubleAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.DOUBLE);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.FLOAT);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfValuesIntAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.INT);
    }
  }

  @Test
  public void
      scan_WithClusteringKeyEndExclusiveRangeOfValuesBigIntAfter_ShouldReturnProperlyResult()
          throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BIGINT);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.BLOB);
    }
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRangeOfValuesTextAfter_ShouldReturnProperlyResult()
      throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
          cKeyTypeBefore, DataType.TEXT);
    }
  }

  public void scan_WithClusteringKeyEndExclusiveRangeOfValues_ShouldReturnProperlyResult(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter) throws ExecutionException {
    RANDOM_GENERATOR.setSeed(781);

    List<Value> valueList = Collections.synchronizedList(new ArrayList<>());
    prepareRecords(valueList, cKeyTypeBefore, cKeyTypeAfter);

    List<Value<?>> expectedValues = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      expectedValues.add(valueList.get(i));
    }

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), valueList.get(9)), false)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC))
            .withOrdering(new Ordering(COL_NAME3, Order.ASC))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Act
    List<Result> scanRet = distributedStorage.scan(scan).all();
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expectedValues);
  }

  protected void prepareRecords(
      List<Value> valueList, DataType cKeyTypeBefore, DataType cKeyTypeAfter)
      throws ExecutionException {
    for (int i = 0; i < 20; i++) {
      Value cKeyValueAfter = getRandomValue(COL_NAME3, cKeyTypeAfter);

      valueList.add(cKeyValueAfter);
      Put put =
          new Put(
                  new Key(getFixedValue(COL_NAME1, DataType.INT)),
                  new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), cKeyValueAfter))
              .withValue(getRandomValue(COL_NAME4, DataType.INT))
              .withValue(getRandomValue(COL_NAME5, DataType.TEXT))
              .forNamespace(getNamespaceName(cKeyTypeBefore))
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
      admin.createNamespace(getNamespaceName(cKeyTypeBefore), true, options);
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        createTable(cKeyTypeBefore, cKeyTypeAfter, options);
      }
    }
  }

  protected static void deleteTestTables() throws ExecutionException {
    for (DataType cKeyTypeBefore : CLUSTERING_KEY_TYPE_LIST) {
      for (DataType cKeyTypeAfter : CLUSTERING_KEY_TYPE_LIST) {
        admin.dropTable(
            getNamespaceName(cKeyTypeBefore), getTableName(cKeyTypeBefore, cKeyTypeAfter));
      }
      admin.dropNamespace(getNamespaceName(cKeyTypeBefore));
    }
  }

  protected static void createTable(
      DataType cKeyTypeBefore, DataType cKeyTypeAfter, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(cKeyTypeBefore),
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

  protected static String getNamespaceName(DataType cKeyTypeBefore) {
    return NAMESPACE_BASE_NAME + cKeyTypeBefore;
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
      if (expectedValue instanceof FloatValue || expectedValue instanceof DoubleValue) {
        assertThat(
                DoubleMath.fuzzyEquals(
                    expectedValue.getAsDouble(), actualValue.getAsDouble(), 1e-4))
            .isTrue();
      } else {
        assertThat(actualValue).isEqualTo(expectedValue);
      }
    }
  }
}
