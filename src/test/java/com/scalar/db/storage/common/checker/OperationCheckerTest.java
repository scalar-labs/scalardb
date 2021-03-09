package com.scalar.db.storage.common.checker;

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
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.util.Utility;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OperationCheckerTest {

  private static final Optional<String> NAMESPACE_PREFIX = Optional.empty();
  private static final Optional<String> NAMESPACE = Optional.of("s1");
  private static final Optional<String> TABLE_NAME = Optional.of("t1");
  private static final String TABLE_FULL_NAME = "s1.t1";
  private static final String PKEY1 = "p1";
  private static final String PKEY2 = "p2";
  private static final String CKEY1 = "c1";
  private static final String CKEY2 = "c2";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  private static final String COL3 = "v3";

  private JdbcTableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Dummy metadata
    metadata =
        new JdbcTableMetadata(
            TABLE_FULL_NAME,
            Arrays.asList(PKEY1, PKEY2),
            Arrays.asList(CKEY1, CKEY2),
            new HashMap<String, Scan.Ordering.Order>() {
              {
                put(CKEY1, Scan.Ordering.Order.ASC);
                put(CKEY2, Scan.Ordering.Order.DESC);
              }
            },
            new LinkedHashMap<String, DataType>() {
              {
                put(PKEY1, DataType.INT);
                put(PKEY2, DataType.TEXT);
                put(CKEY1, DataType.INT);
                put(CKEY2, DataType.TEXT);
                put(COL1, DataType.INT);
                put(COL2, DataType.DOUBLE);
                put(COL3, DataType.BOOLEAN);
              }
            },
            Collections.singletonList(COL1),
            new HashMap<String, Scan.Ordering.Order>() {
              {
                put(COL1, Scan.Ordering.Order.ASC);
              }
            });
  }

  @Test
  public void whenCheckingGetOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(get)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingGetOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new TextValue(PKEY1, "1"), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new TextValue(CKEY1, "2"), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
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
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 1));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 9));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyEndClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
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
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithReverseOrderings_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialOrdering_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithEmptyOrdering_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit);
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKeyRange_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithNegativeLimitNumber_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithInvalidOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartialOrdering_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = -10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withEnd(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit)
            .withOrdering(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(put)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingPutOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = null;
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(put)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("c3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValues_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue("v4", true));
    MutationCondition condition = new PutIfExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValueType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new TextValue(COL1, "1"), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new PutIfNotExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition =
        new PutIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithDeleteIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new DeleteIfExists();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    List<Value> values =
        Arrays.asList(
            new IntValue(COL1, 1), new DoubleValue(COL2, 0.1), new BooleanValue(COL3, true));
    MutationCondition condition = new DeleteIf();
    Put put = new Put(partitionKey, clusteringKey).withValues(values).withCondition(condition);
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new IntValue(1), ConditionalExpression.Operator.EQ));
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(delete)).doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingDeleteOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = null;
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(delete)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new DeleteIfExists();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    MutationCondition condition = new DeleteIfExists();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;
    MutationCondition condition = new DeleteIfExists();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIf();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIfExists();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfNotExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIfNotExists();
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));
    Delete delete = new Delete(partitionKey, clusteringKey).withCondition(condition);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingMutateOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Put put = new Put(partitionKey, clusteringKey).withValue(new IntValue(COL1, 1));
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);
    Delete delete = new Delete(partitionKey, clusteringKey);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(Arrays.asList(put, delete)))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingMutateOperationWithEmptyMutations_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingMutateOperationWithMutationsWithDifferentPartitionKeysWithNotAllowPartitions_shouldThrowMultiPartitionException() {
    // Arrange
    Key partitionKey1 = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key partitionKey2 = new Key(new IntValue(PKEY1, 2), new TextValue(PKEY2, "val2"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val3"));
    Put put = new Put(partitionKey1, clusteringKey).withValue(new IntValue(COL1, 1));
    Utility.setTargetToIfNot(put, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);
    Delete delete = new Delete(partitionKey2, clusteringKey);
    Utility.setTargetToIfNot(delete, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(Arrays.asList(put, delete)))
        .isInstanceOf(MultiPartitionException.class);
  }

  @Test
  public void whenCheckingGetOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(get)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingGetOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new DoubleValue(COL2, 0.1));
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new TextValue(COL1, "1"));
    Key clusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Get get = new Get(partitionKey, clusteringKey).withProjections(projections);
    Utility.setTargetToIfNot(get, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit);
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatCode(() -> new OperationChecker(metadata).check(scan)).doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new DoubleValue(COL2, 0.1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit);
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new TextValue(COL1, "1"));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit);
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val9"));
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    int limit = 10;
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey)
            .withStart(endClusteringKey)
            .withProjections(projections)
            .withLimit(limit);
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(COL1, 1));
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
            .withOrdering(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
    Utility.setTargetToIfNot(scan, NAMESPACE_PREFIX, NAMESPACE, TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> new OperationChecker(metadata).check(scan))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
