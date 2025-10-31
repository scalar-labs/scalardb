package com.scalar.db.dataloader.core.dataimport.dao;

import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_COLUMN_1_PK;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_COLUMN_2_CK;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_COLUMN_4;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_COLUMN_5;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_COLUMN_6;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_NAMESPACE;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_TABLE_NAME;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_VALUE_INT;
import static com.scalar.db.dataloader.core.UnitTestUtils.TEST_VALUE_LONG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ScalarDbDaoTest {

  private static final int TEST_VALUE_INT_MIN = 1;
  private ScalarDbDao dao;
  private DistributedTransactionManager manager;
  private DistributedStorage distributedStorage;

  @BeforeEach
  void setUp() {
    this.dao = new ScalarDbDao();
    this.distributedStorage = mock(DistributedStorage.class);
    this.manager = mock(DistributedTransactionManager.class);
  }

  @Test
  void createScan_scanWithPartitionKey_shouldCreateScanObjectWithPartitionKey() {

    // Create Scan Object
    Scan scan =
        this.dao.createScan(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            new ArrayList<>(),
            0);

    // Create expected result
    Scan expectedResult =
        generateScanResult(
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            new ArrayList<>(),
            0);

    // Compare Scan object
    assertThat(scan.toString()).isEqualTo(expectedResult.toString());
  }

  @Test
  void createScan_scanWithLimitAndProjection_shouldCreateScanObjectWithLimitAndProjection() {

    // Create Scan Object
    Scan scan =
        this.dao.createScan(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            Arrays.asList(TEST_COLUMN_4, TEST_COLUMN_5, TEST_COLUMN_6),
            5);

    // Create expected result
    Scan expectedResult =
        generateScanResult(
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            Arrays.asList(TEST_COLUMN_4, TEST_COLUMN_5, TEST_COLUMN_6),
            5);

    // Compare Scan object
    assertThat(scan.toString()).isEqualTo(expectedResult.toString());
  }

  @Test
  void createScan_scanWithScanRangeAndOrder_shouldCreateScanObjectWithSortAndRange() {

    // Create Scan Object
    Scan scan =
        this.dao.createScan(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(
                Key.newBuilder().addInt(TEST_COLUMN_2_CK, TEST_VALUE_INT_MIN).build(),
                Key.newBuilder().addInt(TEST_COLUMN_2_CK, TEST_VALUE_INT).build(),
                true,
                false),
            Arrays.asList(Scan.Ordering.asc(TEST_COLUMN_2_CK)),
            new ArrayList<>(),
            0);
    // Create expected result
    Scan expectedResult =
        generateScanResult(
            Key.newBuilder().addBigInt(TEST_COLUMN_1_PK, TEST_VALUE_LONG).build(),
            new ScanRange(
                Key.newBuilder().addInt(TEST_COLUMN_2_CK, TEST_VALUE_INT_MIN).build(),
                Key.newBuilder().addInt(TEST_COLUMN_2_CK, TEST_VALUE_INT).build(),
                true,
                false),
            Arrays.asList(Scan.Ordering.asc(TEST_COLUMN_2_CK)),
            new ArrayList<>(),
            0);
    // Compare Scan object
    assertThat(scan.toString()).isEqualTo(expectedResult.toString());
  }

  @Test
  void createScan_scanWithoutPartitionKey_shouldCreateScanAllObject() {

    // Create Scan Object
    Scan scan =
        this.dao.createScan(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            null,
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            new ArrayList<>(),
            0);

    // Create expected result
    Scan expectedResult = generateScanAllResult(new ArrayList<>(), 0);

    // Compare ScanAll object
    assertThat(scan.toString()).isEqualTo(expectedResult.toString());
  }

  @Test
  void createScan_scanAllWithLimitAndProjection_shouldCreateScanAllObjectWithLimitAndProjection() {

    // Create Scan Object
    Scan scan =
        this.dao.createScan(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            null,
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            Arrays.asList(TEST_COLUMN_4, TEST_COLUMN_5, TEST_COLUMN_6),
            5);

    // Create expected result
    Scan expectedResult =
        generateScanAllResult(Arrays.asList(TEST_COLUMN_4, TEST_COLUMN_5, TEST_COLUMN_6), 5);

    // Compare ScanAll object
    assertThat(scan.toString()).isEqualTo(expectedResult.toString());
  }

  @Test
  void createScanner_withTransactionManager_ShouldCreateScannerObject()
      throws CrudException, ScalarDbDaoException {
    // Create Scan Object
    TransactionManagerCrudOperable.Scanner mockScanner =
        Mockito.mock(TransactionManagerCrudOperable.Scanner.class);
    when(manager.getScanner(any())).thenReturn(mockScanner);
    TransactionManagerCrudOperable.Scanner result =
        this.dao.createScanner(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            null,
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            new ArrayList<>(),
            0,
            manager);
    // Assert
    assertNotNull(result);
    assertEquals(mockScanner, result);
    result = this.dao.createScanner(TEST_NAMESPACE, TEST_TABLE_NAME, null, 0, manager);
    // Assert
    assertNotNull(result);
    assertEquals(mockScanner, result);
  }

  @Test
  void createScanner_withStorage_ShouldCreateScannerObject()
      throws CrudException, ExecutionException, ScalarDbDaoException {
    // Create Scan Object
    Scanner mockScanner = mock(Scanner.class);
    when(distributedStorage.scan(any())).thenReturn(mockScanner);
    Scanner result =
        this.dao.createScanner(
            TEST_NAMESPACE,
            TEST_TABLE_NAME,
            null,
            new ScanRange(null, null, false, false),
            new ArrayList<>(),
            new ArrayList<>(),
            0,
            distributedStorage);
    // Assert
    assertNotNull(result);
    assertEquals(mockScanner, result);

    result = this.dao.createScanner(TEST_NAMESPACE, TEST_TABLE_NAME, null, 0, distributedStorage);
    // Assert
    assertNotNull(result);
    assertEquals(mockScanner, result);
  }

  /**
   * Create Scan Object
   *
   * @param partitionKey Partition key used in ScalarDB scan
   * @param range Optional range to set ScalarDB scan start and end values
   * @param sorts Optional scan clustering key sorting values
   * @param projections List of column projection to use during scan
   * @param limit Scan limit value
   * @return ScalarDB scan instance
   */
  private Scan generateScanResult(
      Key partitionKey,
      ScanRange range,
      List<Scan.Ordering> sorts,
      List<String> projections,
      int limit) {
    ScanBuilder.BuildableScan scan =
        Scan.newBuilder()
            .namespace(TEST_NAMESPACE)
            .table(TEST_TABLE_NAME)
            .partitionKey(partitionKey);

    // Set boundary start
    if (range.getScanStartKey() != null) {
      scan.start(range.getScanStartKey(), range.isStartInclusive());
    }

    // with end
    if (range.getScanEndKey() != null) {
      scan.end(range.getScanEndKey(), range.isEndInclusive());
    }

    // clustering order
    for (Scan.Ordering sort : sorts) {
      scan.ordering(sort);
    }

    // projections
    if (projections != null && !projections.isEmpty()) {
      scan.projections(projections);
    }

    // limit
    if (limit > 0) {
      scan.limit(limit);
    }
    return scan.build();
  }

  /**
   * Create ScanAll Object
   *
   * @param projections List of column projection to use during scan
   * @param limit Scan limit value
   * @return ScalarDB scan instance
   */
  private Scan generateScanAllResult(List<String> projections, int limit) {
    ScanBuilder.BuildableScanAll scan =
        Scan.newBuilder().namespace(TEST_NAMESPACE).table(TEST_TABLE_NAME).all();

    // projections
    if (projections != null && !projections.isEmpty()) {
      scan.projections(projections);
    }

    // limit
    if (limit > 0) {
      scan.limit(limit);
    }
    return scan.build();
  }
}
