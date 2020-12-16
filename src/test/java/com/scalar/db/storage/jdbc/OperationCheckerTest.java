package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
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
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.TableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class OperationCheckerTest {

  private static final Table TABLE = new Table("s1", "t1");
  private static final String PKEY1 = "p1";
  private static final String PKEY2 = "p2";
  private static final String CKEY1 = "c1";
  private static final String CKEY2 = "c2";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  private static final String COL3 = "v3";

  @Mock private TableMetadataManager tableMetadataManager;

  private OperationChecker operationChecker;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Dummy metadata
    TableMetadata dummyTableMetadata =
        new TableMetadata(
            TABLE,
            new HashMap<String, DataType>() {
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
            Arrays.asList(PKEY1, PKEY2),
            Arrays.asList(CKEY1, CKEY2),
            new HashMap<String, Scan.Ordering.Order>() {
              {
                put(CKEY1, Scan.Ordering.Order.ASC);
                put(CKEY2, Scan.Ordering.Order.DESC);
              }
            },
            new HashSet<String>() {
              {
                add(COL1);
              }
            });

    when(tableMetadataManager.getTableMetadata(TABLE)).thenReturn(dummyTableMetadata);

    operationChecker = new OperationChecker(tableMetadataManager);
  }

  @Test
  public void whenCheckingGetOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatCode(() -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingGetOperationWithInvalidTable_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = new Table("s1", "t2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingGetOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidPartitionKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new TextValue(PKEY1, "1"), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingGetOperationWithoutAnyPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = null;
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithInvalidClusteringKeyType_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new TextValue(CKEY1, "2"), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 1));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 9));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithoutAnyEndClusteringKey_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithReverseOrderings_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithPartialOrdering_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
          }
        };

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithEmptyOrdering_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings = Collections.emptyList();

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingScanOperationWithInvalidTable_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = new Table("s1", "t2");
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidProjections_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, "v4");
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithoutAnyPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = null;
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidClusteringKeyRange_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2));
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithNegativeLimitNumber_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = -10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingScanOperationWithInvalidOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = -10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.DESC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithInvalidPartialOrdering_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val9"));
    int limit = -10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.ASC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfNotExists();

    // Act Assert
    assertThatCode(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingPutOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = null;

    // Act Assert
    assertThatCode(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("c3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithoutAnyPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = null;
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfNotExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfNotExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValues_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put("v4", new BooleanValue("v4", true));
          }
        };
    MutationCondition condition = new PutIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithInvalidValueType_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new TextValue(COL1, "1"));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new PutIfNotExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithInvalidPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition =
        new PutIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingPutOperationWithDeleteIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingPutOperationWithDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Map<String, Value> values =
        new HashMap<String, Value>() {
          {
            put(COL1, new IntValue(COL1, 1));
            put(COL2, new DoubleValue(COL2, 0.1));
            put(COL3, new BooleanValue(COL3, true));
          }
        };
    MutationCondition condition = new DeleteIf();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkPut(table, partitionKey, clusteringKey, values, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new IntValue(1), ConditionalExpression.Operator.EQ));
    ;

    // Act Assert
    assertThatCode(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingDeleteOperationWithoutAnyCondition_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = null;

    // Act Assert
    assertThatCode(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingDeleteOperationWithInvalidTable_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = new Table("s1", "t2");
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue("p3", "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithoutAnyPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = null;
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithoutAnyClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = null;
    MutationCondition condition = new DeleteIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingDeleteOperationWithPutIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIf();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIfExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithPutIfNotExistsCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition = new PutIfNotExists();

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingDeleteOperationWithInvalidDeleteIfCondition_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    MutationCondition condition =
        new DeleteIf(
            new ConditionalExpression(COL1, new TextValue("1"), ConditionalExpression.Operator.EQ));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkDelete(table, partitionKey, clusteringKey, condition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenCheckingMutateOperationWithAllValidArguments_shouldNotThrowAnyException() {
    // Arrange
    Key partitionKey = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val1"));
    Put put = new Put(partitionKey, clusteringKey).withValue(new IntValue(COL1, 1));
    Delete delete = new Delete(partitionKey, clusteringKey);

    // Act Assert
    assertThatCode(() -> operationChecker.checkMutate(Arrays.asList(put, delete)))
        .doesNotThrowAnyException();
  }

  @Test
  public void whenCheckingMutateOperationWithEmptyMutations_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> operationChecker.checkMutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingMutateOperationWithMutationsWithDifferentPartitionKeys_shouldThrowMultiPartitionException() {
    // Arrange
    Key partitionKey1 = new Key(new IntValue(PKEY1, 1), new TextValue(PKEY2, "val1"));
    Key partitionKey2 = new Key(new IntValue(PKEY1, 2), new TextValue(PKEY2, "val2"));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val3"));
    Put put = new Put(partitionKey1, clusteringKey).withValue(new IntValue(COL1, 1));
    Delete delete = new Delete(partitionKey2, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.checkMutate(Arrays.asList(put, delete)))
        .isInstanceOf(MultiPartitionException.class);
  }

  @Test
  public void whenCheckingGetOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key clusteringKey = null;

    // Act Assert
    assertThatCode(() -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingGetOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new DoubleValue(COL2, 0.1));
    Key clusteringKey = null;

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new TextValue(COL1, "1"));
    Key clusteringKey = null;

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingGetOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key clusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue(CKEY2, "val2"));

    // Act Assert
    assertThatThrownBy(
            () -> operationChecker.checkGet(table, projections, partitionKey, clusteringKey))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKey_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings = Collections.emptyList();

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithValidOrderings_shouldNotThrowAnyException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings =
        Collections.singletonList(new Scan.Ordering(COL1, Scan.Ordering.Order.DESC));

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      whenCheckingScanOperationWithNonIndexedColumnAsPartitionKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new DoubleValue(COL2, 0.1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings = Collections.emptyList();

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyButWrongType_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new TextValue(COL1, "1"));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings = Collections.emptyList();

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithClusteringKey_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val1"));
    Key endClusteringKey = new Key(new IntValue(CKEY1, 2), new TextValue("c3", "val9"));
    int limit = 10;
    List<Scan.Ordering> orderings = Collections.emptyList();

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      whenCheckingScanOperationWithIndexedColumnAsPartitionKeyWithInvalidOrderings_shouldThrowIllegalArgumentException() {
    // Arrange
    Table table = TABLE;
    List<String> projections = Arrays.asList(COL1, COL2, COL3);
    Key partitionKey = new Key(new IntValue(COL1, 1));
    Key startClusteringKey = null;
    Key endClusteringKey = null;
    int limit = 10;
    List<Scan.Ordering> orderings =
        new ArrayList<Scan.Ordering>() {
          {
            add(new Scan.Ordering(CKEY1, Scan.Ordering.Order.ASC));
            add(new Scan.Ordering(CKEY2, Scan.Ordering.Order.DESC));
          }
        };

    // Act Assert
    assertThatThrownBy(
            () ->
                operationChecker.checkScan(
                    table,
                    projections,
                    partitionKey,
                    startClusteringKey,
                    endClusteringKey,
                    limit,
                    orderings))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
