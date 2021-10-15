package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@SuppressFBWarnings(
    value = {"MS_CANNOT_BE_FINAL", "MS_PKGPROTECT", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class StorageMultipleClusteringKeysIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "integration_testing_";
  protected static final String TABLE_BASE_NAME = "test_table_mul_key_";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final int DATA_NUM = 20;
  protected static final ImmutableList<DataType> CLUSTERING_KEY_TYPE_LIST =
      ImmutableList.of(
          DataType.BOOLEAN,
          DataType.INT,
          DataType.BIGINT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TEXT,
          DataType.BLOB);
  private static final Random RANDOM_GENERATOR = new Random();

  private static boolean initialized;
  protected static DistributedStorageAdmin admin;
  protected static DistributedStorage storage;
  protected static String namespaceBaseName;
  protected static List<DataType> clusteringKeyTypeList;
  private static long seed;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory = new StorageFactory(getDatabaseConfig());
      admin = factory.getAdmin();
      namespaceBaseName = getNamespaceBaseName();
      clusteringKeyTypeList = getClusteringKeyTypeList();
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println(
          "The seed used in the multiple clustering keys integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected List<DataType> getClusteringKeyTypeList() {
    return CLUSTERING_KEY_TYPE_LIST;
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      admin.createNamespace(getNamespaceName(cKeyTypeBefore), true, options);
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        createTable(cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, options);
      }
    }
  }

  private void createTable(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(cKeyTypeBefore),
        getTableName(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter),
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, cKeyTypeBefore)
            .addColumn(COL_NAME3, cKeyTypeAfter)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.TEXT)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME2, cKeyClusteringOrderBefore)
            .addClusteringKey(COL_NAME3, cKeyClusteringOrderAfter)
            .build(),
        true,
        options);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    admin.close();
    storage.close();
  }

  private static void deleteTables() throws ExecutionException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        admin.dropTable(
            getNamespaceName(cKeyTypeBefore),
            getTableName(cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC));
      }
      admin.dropNamespace(getNamespaceName(cKeyTypeBefore));
    }
  }

  private void truncateTable(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(cKeyTypeBefore),
        getTableName(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter));
  }

  private static String getTableName(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter) {
    return TABLE_BASE_NAME
        + String.join(
            "_",
            cKeyTypeBefore.toString(),
            cKeyClusteringOrderBefore.toString(),
            cKeyTypeAfter.toString(),
            cKeyClusteringOrderAfter.toString());
  }

  private static String getNamespaceName(DataType cKeyTypeBefore) {
    return namespaceBaseName + cKeyTypeBefore;
  }

  @Test
  public void scan_WithoutClusteringKeys_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithoutClusteringKeys_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithoutClusteringKeys_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithoutClusteringKeys_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList;
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanStartIndex;
    int scanEndIndex;
    Value<?> scanStartValue;
    Value<?> endStartValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanEndIndex = valueList.size();
      scanStartValue = new BooleanValue(COL_NAME2, false);
      endStartValue = new BooleanValue(COL_NAME2, true);
    } else {
      scanStartIndex = 5;
      scanEndIndex = 15;
      scanStartValue = valueList.get(scanStartIndex);
      endStartValue = valueList.get(scanEndIndex - 1);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(scanStartValue), true)
            .withEnd(new Key(endStartValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(scanStartIndex, scanEndIndex);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanStartIndex;
    int scanEndIndex;
    Value<?> scanStartValue;
    Value<?> endStartValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanEndIndex = valueList.size();
      scanStartValue = new BooleanValue(COL_NAME2, false);
      endStartValue = new BooleanValue(COL_NAME2, true);
    } else {
      scanStartIndex = 5;
      scanEndIndex = 15;
      scanStartValue = valueList.get(scanStartIndex);
      endStartValue = valueList.get(scanEndIndex - 1);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(scanStartValue), false)
            .withEnd(new Key(endStartValue), false)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(scanStartIndex + 1, scanEndIndex - 1);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyStartInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanStartIndex;
    Value<?> scanStartValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanStartValue = new BooleanValue(COL_NAME2, false);
    } else {
      scanStartIndex = 10;
      scanStartValue = valueList.get(scanStartIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(scanStartValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(scanStartIndex, valueList.size());
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyStartExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanStartIndex;
    Value<?> scanStartValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanStartValue = new BooleanValue(COL_NAME2, false);
    } else {
      scanStartIndex = 10;
      scanStartValue = valueList.get(scanStartIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(scanStartValue), false)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(scanStartIndex + 1, valueList.size());
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyEndInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanEndIndex;
    Value<?> scanEndValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanEndIndex = valueList.size() - 1;
      scanEndValue = new BooleanValue(COL_NAME2, true);
    } else {
      scanEndIndex = 10;
      scanEndValue = valueList.get(scanEndIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(scanEndValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(0, scanEndIndex + 1);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  @Test
  public void scan_WithBeforeClusteringKeyEndExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      scan_WithBeforeClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, false);
      scan_WithBeforeClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
          cKeyTypeBefore, Order.ASC, true);
    }
  }

  private void scan_WithBeforeClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, DataType.INT, cKeyClusteringOrderBefore);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecordsForScanWithBeforeClusteringKeyRange(
            cKeyTypeBefore, cKeyClusteringOrderBefore);

    int scanEndIndex;
    Value<?> scanEndValue;
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      scanEndIndex = 1;
      scanEndValue = new BooleanValue(COL_NAME2, true);
    } else {
      scanEndIndex = 10;
      scanEndValue = valueList.get(scanEndIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(scanEndValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    DataType.INT,
                    cKeyClusteringOrderBefore));
    List<Value<?>> expected = valueList.subList(0, scanEndIndex + 1);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME2, expected);
  }

  private List<Value<?>> prepareRecordsForScanWithBeforeClusteringKeyRange(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore) throws ExecutionException {
    List<Value<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();
    if (cKeyTypeBefore == DataType.BOOLEAN) {
      Arrays.asList(new BooleanValue(COL_NAME2, true), new BooleanValue(COL_NAME2, false))
          .forEach(
              v -> {
                ret.add(v);
                puts.add(
                    preparePutForScanWithBeforeClusteringKeyRange(
                        cKeyTypeBefore, cKeyClusteringOrderBefore, v));
              });
    } else {
      for (int i = 0; i < DATA_NUM; i++) {
        Value<?> cKeyValueBefore = getRandomValue(COL_NAME2, cKeyTypeBefore);
        ret.add(cKeyValueBefore);
        puts.add(
            preparePutForScanWithBeforeClusteringKeyRange(
                cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyValueBefore));
      }
    }
    try {
      storage.mutate(puts);
    } catch (ExecutionException e) {
      throw new ExecutionException("put data to database failed", e);
    }
    ret.sort(comparator());
    return ret;
  }

  private Put preparePutForScanWithBeforeClusteringKeyRange(
      DataType cKeyTypeBefore, Order cKeyClusteringOrderBefore, Value<?> cKeyValueBefore) {
    return new Put(
            new Key(getFixedValue(COL_NAME1, DataType.INT)),
            new Key(cKeyValueBefore, getFixedValue(COL_NAME3, DataType.INT)))
        .withValue(getRandomValue(COL_NAME4, DataType.INT))
        .withValue(getRandomValue(COL_NAME5, DataType.TEXT))
        .forNamespace(getNamespaceName(cKeyTypeBefore))
        .forTable(
            getTableName(
                cKeyTypeBefore,
                cKeyClusteringOrderBefore,
                DataType.INT,
                cKeyClusteringOrderBefore));
  }

  @Test
  public void scan_WithClusteringKeyInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanStartIndex;
    int scanEndIndex;
    Value<?> scanStartValue;
    Value<?> endStartValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanEndIndex = valueList.size();
      scanStartValue = new BooleanValue(COL_NAME3, false);
      endStartValue = new BooleanValue(COL_NAME3, true);
    } else {
      scanStartIndex = 5;
      scanEndIndex = 15;
      scanStartValue = valueList.get(scanStartIndex);
      endStartValue = valueList.get(scanEndIndex - 1);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanStartValue), true)
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), endStartValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(scanStartIndex, scanEndIndex);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithClusteringKeyExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);

    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanStartIndex;
    int scanEndIndex;
    Value<?> scanStartValue;
    Value<?> endStartValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanEndIndex = valueList.size();
      scanStartValue = new BooleanValue(COL_NAME3, false);
      endStartValue = new BooleanValue(COL_NAME3, true);
    } else {
      scanStartIndex = 5;
      scanEndIndex = 15;
      scanStartValue = valueList.get(scanStartIndex);
      endStartValue = valueList.get(scanEndIndex - 1);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanStartValue), false)
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), endStartValue), false)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(scanStartIndex + 1, scanEndIndex - 1);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithClusteringKeyStartInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyStartInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);
    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanStartIndex;
    Value<?> scanStartValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanStartValue = new BooleanValue(COL_NAME3, false);
    } else {
      scanStartIndex = 10;
      scanStartValue = valueList.get(scanStartIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanStartValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(scanStartIndex, valueList.size());
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithClusteringKeyStartExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyStartExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);
    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanStartIndex;
    Value<?> scanStartValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanStartIndex = 0;
      scanStartValue = new BooleanValue(COL_NAME3, false);
    } else {
      scanStartIndex = 10;
      scanStartValue = valueList.get(scanStartIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withStart(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanStartValue), false)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(scanStartIndex + 1, valueList.size());
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithClusteringKeyEndInclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyEndInclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);
    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanEndIndex;
    Value<?> scanEndValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanEndIndex = valueList.size() - 1;
      scanEndValue = new BooleanValue(COL_NAME3, true);
    } else {
      scanEndIndex = 10;
      scanEndValue = valueList.get(scanEndIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanEndValue), true)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(0, scanEndIndex + 1);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  @Test
  public void scan_WithClusteringKeyEndExclusiveRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType cKeyTypeBefore : clusteringKeyTypeList) {
      for (DataType cKeyTypeAfter : clusteringKeyTypeList) {
        scan_WithClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, false);
        scan_WithClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
            cKeyTypeBefore, Order.ASC, cKeyTypeAfter, Order.ASC, true);
      }
    }
  }

  private void scan_WithClusteringKeyEndExclusiveRange_ShouldReturnProperResult(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      boolean reverse)
      throws ExecutionException, IOException {
    truncateTable(
        cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);
    RANDOM_GENERATOR.setSeed(seed);
    List<Value<?>> valueList =
        prepareRecords(
            cKeyTypeBefore, cKeyClusteringOrderBefore, cKeyTypeAfter, cKeyClusteringOrderAfter);

    int scanEndIndex;
    Value<?> scanEndValue;
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      scanEndIndex = 1;
      scanEndValue = new BooleanValue(COL_NAME3, true);
    } else {
      scanEndIndex = 10;
      scanEndValue = valueList.get(scanEndIndex);
    }
    Scan scan =
        new Scan(new Key(getFixedValue(COL_NAME1, DataType.INT)))
            .withEnd(new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), scanEndValue), false)
            .withOrdering(
                new Ordering(
                    COL_NAME2,
                    reverse ? reverseOrder(cKeyClusteringOrderBefore) : cKeyClusteringOrderBefore))
            .withOrdering(
                new Ordering(
                    COL_NAME3,
                    reverse ? reverseOrder(cKeyClusteringOrderAfter) : cKeyClusteringOrderAfter))
            .forNamespace(getNamespaceName(cKeyTypeBefore))
            .forTable(
                getTableName(
                    cKeyTypeBefore,
                    cKeyClusteringOrderBefore,
                    cKeyTypeAfter,
                    cKeyClusteringOrderAfter));
    List<Value<?>> expected = valueList.subList(0, scanEndIndex);
    if (reverse) {
      expected.sort(reverseComparator());
    }

    // Act
    List<Result> scanRet = scanAll(scan);

    // Assert
    assertScanResultWithOrdering(scanRet, COL_NAME3, expected);
  }

  private List<Value<?>> prepareRecords(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter)
      throws ExecutionException {
    List<Value<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();
    if (cKeyTypeAfter == DataType.BOOLEAN) {
      Arrays.asList(new BooleanValue(COL_NAME3, true), new BooleanValue(COL_NAME3, false))
          .forEach(
              v -> {
                ret.add(v);
                puts.add(
                    preparePut(
                        cKeyTypeBefore,
                        cKeyClusteringOrderBefore,
                        cKeyTypeAfter,
                        cKeyClusteringOrderAfter,
                        v));
              });
    } else {
      for (int i = 0; i < DATA_NUM; i++) {
        Value<?> cKeyValueAfter = getRandomValue(COL_NAME3, cKeyTypeAfter);
        ret.add(cKeyValueAfter);
        puts.add(
            preparePut(
                cKeyTypeBefore,
                cKeyClusteringOrderBefore,
                cKeyTypeAfter,
                cKeyClusteringOrderAfter,
                cKeyValueAfter));
      }
    }
    try {
      storage.mutate(puts);
    } catch (ExecutionException e) {
      throw new ExecutionException("put data to database failed", e);
    }
    ret.sort(comparator());
    return ret;
  }

  private Put preparePut(
      DataType cKeyTypeBefore,
      Order cKeyClusteringOrderBefore,
      DataType cKeyTypeAfter,
      Order cKeyClusteringOrderAfter,
      Value<?> cKeyValueAfter) {
    return new Put(
            new Key(getFixedValue(COL_NAME1, DataType.INT)),
            new Key(getFixedValue(COL_NAME2, cKeyTypeBefore), cKeyValueAfter))
        .withValue(getRandomValue(COL_NAME4, DataType.INT))
        .withValue(getRandomValue(COL_NAME5, DataType.TEXT))
        .forNamespace(getNamespaceName(cKeyTypeBefore))
        .forTable(
            getTableName(
                cKeyTypeBefore,
                cKeyClusteringOrderBefore,
                cKeyTypeAfter,
                cKeyClusteringOrderAfter));
  }

  private Order reverseOrder(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError();
    }
  }

  private Comparator<Value<?>> comparator() {
    return this::compareTo;
  }

  private Comparator<Value<?>> reverseComparator() {
    return (l, r) -> -1 * compareTo(l, r);
  }

  private int compareTo(Value<?> l, Value<?> r) {
    if (l.getClass() != r.getClass()) {
      throw new IllegalArgumentException("the types of the values are different");
    }
    if (l instanceof BooleanValue) {
      return ((BooleanValue) l).compareTo((BooleanValue) r);
    } else if (l instanceof IntValue) {
      return ((IntValue) l).compareTo((IntValue) r);
    } else if (l instanceof BigIntValue) {
      return ((BigIntValue) l).compareTo((BigIntValue) r);
    } else if (l instanceof FloatValue) {
      return ((FloatValue) l).compareTo((FloatValue) r);
    } else if (l instanceof DoubleValue) {
      return ((DoubleValue) l).compareTo((DoubleValue) r);
    } else if (l instanceof TextValue) {
      return ((TextValue) l).compareTo((TextValue) r);
    } else if (l instanceof BlobValue) {
      return ((BlobValue) l).compareTo((BlobValue) r);
    } else {
      throw new AssertionError();
    }
  }

  private Value<?> getRandomValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(
            columnName, nextLongBetween(BigIntValue.MIN_VALUE, BigIntValue.MAX_VALUE));
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
      case BOOLEAN:
        return new BooleanValue(columnName, RANDOM_GENERATOR.nextBoolean());
      default:
        throw new RuntimeException("Unsupported data type for random generating");
    }
  }

  public long nextLongBetween(long min, long max) {
    OptionalLong randomLong = RANDOM_GENERATOR.longs(min, (max + 1)).limit(1).findFirst();
    return randomLong.orElse(0);
  }

  private Value<?> getFixedValue(String columnName, DataType dataType) {
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
      case BOOLEAN:
        return new BooleanValue(columnName, true);
      default:
        throw new RuntimeException("Unsupported data type");
    }
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  private void assertScanResultWithOrdering(
      List<Result> actual, String checkedColumn, List<Value<?>> expectedValues) {
    assertThat(actual.size()).isEqualTo(expectedValues.size());

    for (int i = 0; i < actual.size(); i++) {
      Value<?> expectedValue = expectedValues.get(i);
      Result actualResult = actual.get(i);
      assertThat(actualResult.getValue(checkedColumn).isPresent()).isTrue();
      Value<?> actualValue = actualResult.getValue(checkedColumn).get();
      assertThat(actualValue).isEqualTo(expectedValue);
    }
  }
}
