package com.scalar.db.storage.common.checker;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.TableMetadataManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OperationCheckerTest {

  private static final String NAMESPACE = "s1";
  private static final String TABLE_NAME = "t1";
  private static final String PKEY1 = "p1";
  private static final String PKEY2 = "p2";
  private static final String CKEY1 = "c1";
  private static final String CKEY2 = "c2";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  private static final String COL3 = "v3";

  @Mock private TableMetadataManager metadataManager;
  private OperationChecker operationChecker;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Dummy metadata
    when(metadataManager.getTableMetadata(any()))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn(PKEY1, DataType.INT)
                .addColumn(PKEY2, DataType.TEXT)
                .addColumn(CKEY1, DataType.INT)
                .addColumn(CKEY2, DataType.TEXT)
                .addColumn(COL1, DataType.INT)
                .addColumn(COL2, DataType.DOUBLE)
                .addColumn(COL3, DataType.BOOLEAN)
                .addPartitionKey(PKEY1)
                .addPartitionKey(PKEY2)
                .addClusteringKey(CKEY1, Scan.Ordering.Order.ASC)
                .addClusteringKey(CKEY2, Scan.Ordering.Order.DESC)
                .addSecondaryIndex(COL1)
                .build());

    operationChecker = new OperationChecker(metadataManager);
  }

  @Test
  public void whenCheckingOperationWithWrongTable_shouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Returning null means table not found
    when(metadataManager.getTableMetadata(any())).thenReturn(null);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingGetOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(get)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingGetOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, "p3", "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, "1", PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, "c3", "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, "2", CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 1);
    Key endClusteringKey = new Key(CKEY1, 9);
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyEndClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithReverseOrderings_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialOrdering_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithEmptyOrdering_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, "p3", "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, "c3", "val1");
    Key endClusteringKey = new Key(CKEY1, 2, "c3", "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKeyRange_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2);
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithNegativeLimitNumber_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithInvalidOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartialOrdering_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key startClusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Key endClusteringKey = new Key(CKEY1, 2, CKEY2, "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingPutOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = null;
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, "c3", "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, "c3", "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = null;
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValues_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue("v4", true));
    MutationCondition condition = new PutIfExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValueType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new TextValue(COL1, "1"), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition =
        new PutIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithDeleteIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new DeleteIfExists();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new DeleteIf();
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithPartitionKeyWithNullTextValue_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = Key.newBuilder().addInt(PKEY1, 1).addText(PKEY2, null).build();
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithPartitionKeyWithEmptyTextValue_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithClusteringKeyWithNullTextValue_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = Key.newBuilder().addInt(CKEY1, 2).addText(CKEY2, null).build();
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithClusteringKeyWithEmptyTextValue_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "");
    List<Value<?>> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithPartitionKeyWithNullBlobValue_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any()))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn(PKEY1, DataType.BLOB)
                .addColumn(CKEY1, DataType.BLOB)
                .addColumn(COL1, DataType.INT)
                .addPartitionKey(PKEY1)
                .addClusteringKey(CKEY1)
                .build());

    operationChecker = new OperationChecker(metadataManager);

    Key partitionKey = new Key(PKEY1, (byte[]) null);
    Key clusteringKey = new Key(CKEY1, new byte[] {1, 1, 1});
    List<Value<?>> values = Collections.singletonList(new IntValue(COL1, 1));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithPartitionKeyWithEmptyBlobValue_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any()))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn(PKEY1, DataType.BLOB)
                .addColumn(CKEY1, DataType.BLOB)
                .addColumn(COL1, DataType.INT)
                .addPartitionKey(PKEY1)
                .addClusteringKey(CKEY1)
                .build());

    operationChecker = new OperationChecker(metadataManager);

    Key partitionKey = new Key(PKEY1, new byte[0]);
    Key clusteringKey = new Key(CKEY1, new byte[] {1, 1, 1});
    List<Value<?>> values = Collections.singletonList(new IntValue(COL1, 1));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithClusteringKeyWithNullBlobValue_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any()))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn(PKEY1, DataType.BLOB)
                .addColumn(CKEY1, DataType.BLOB)
                .addColumn(COL1, DataType.INT)
                .addPartitionKey(PKEY1)
                .addClusteringKey(CKEY1)
                .build());

    operationChecker = new OperationChecker(metadataManager);

    Key partitionKey = new Key(PKEY1, new byte[] {1, 1, 1});
    Key clusteringKey = new Key(CKEY1, (byte[]) null);
    List<Value<?>> values = Collections.singletonList(new IntValue(COL1, 1));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithClusteringKeyWithEmptyBlobValue_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any()))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn(PKEY1, DataType.BLOB)
                .addColumn(CKEY1, DataType.BLOB)
                .addColumn(COL1, DataType.INT)
                .addPartitionKey(PKEY1)
                .addClusteringKey(CKEY1)
                .build());

    operationChecker = new OperationChecker(metadataManager);

    Key partitionKey = new Key(PKEY1, new byte[] {1, 1, 1});
    Key clusteringKey = new Key(CKEY1, new byte[0]);
    List<Value<?>> values = Collections.singletonList(new IntValue(COL1, 1));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValues(values)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new IntValue(1), ConditionalExpression.Operator.EQ));
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(delete)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingDeleteOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition = null;
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(delete)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, "p3", "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition = new DeleteIfExists();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, "c3", "val1");
    MutationCondition condition = new DeleteIfExists();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = null;
    MutationCondition condition = new DeleteIfExists();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition = new PutIf();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition = new PutIfExists();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfNotExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition = new PutIfNotExists();
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .withCondition(condition)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingMutateOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val1");
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL1, 1)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);
    Delete delete =
        new Delete(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(Arrays.asList(put, delete)))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingMutateOperationWithEmptyMutations_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingMutateOperationWithMutationsWithDifferentPartitionKeys_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey1 = new Key(PKEY1, 1, PKEY2, "val1");
    Key partitionKey2 = new Key(PKEY1, 2, PKEY2, "val2");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val3");
    Put put =
        new Put(partitionKey1, clusteringKey)
            .withValue(COL1, 1)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);
    Delete delete =
        new Delete(partitionKey2, clusteringKey).forNamespace(NAMESPACE).forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(Arrays.asList(put, delete)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingMutateOperationWithMutationsWithSameTableAndPartitionKeyButDifferentNamespace_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(PKEY1, 1, PKEY2, "val1");
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val3");
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL1, 1)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);
    Delete delete = new Delete(partitionKey, clusteringKey).forNamespace("s2").forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(Arrays.asList(put, delete)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingGetOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(COL1, 1);
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(get)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingGetOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL2, 0.1d);
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL1, "1");
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL1, 1);
    Key clusteringKey = new Key(CKEY1, 2, CKEY2, "val2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get =
        new Get(partitionKey, clusteringKey)
            .withProjections(projections)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(COL1, 1);
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL2, 0.1d);
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL1, "1");
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL1, 1);
    Key startClusteringKey = new Key(CKEY1, 2, "c3", "val1");
    Key endClusteringKey = new Key(CKEY1, 2, "c3", "val9");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(COL1, 1);
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .forNamespace(NAMESPACE)
            .forTable(TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
