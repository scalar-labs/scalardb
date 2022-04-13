package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementValidatorTest {

  private static final String NAMESPACE_NAME = "ns";
  private static final String TABLE_NAME = "tbl";
  private static final com.scalar.db.api.TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", com.scalar.db.io.DataType.TEXT)
          .addColumn("p2", com.scalar.db.io.DataType.TEXT)
          .addColumn("c1", com.scalar.db.io.DataType.TEXT)
          .addColumn("c2", com.scalar.db.io.DataType.TEXT)
          .addColumn("col1", com.scalar.db.io.DataType.TEXT)
          .addColumn("col2", com.scalar.db.io.DataType.TEXT)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .addClusteringKey("c1", Scan.Ordering.Order.ASC)
          .addClusteringKey("c2", Scan.Ordering.Order.DESC)
          .addSecondaryIndex("col2")
          .build();

  @Mock private DistributedTransactionAdmin admin;

  private StatementValidator statementValidator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(admin.namespaceExists(NAMESPACE_NAME)).thenReturn(true);
    when(admin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME)).thenReturn(TABLE_METADATA);

    statementValidator = new StatementValidator(Metadata.create(admin, -1));
  }

  @Test
  public void validate_ProperCreateTableStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    CreateTableStatement statement1 =
        CreateTableStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            true,
            ImmutableMap.of(
                "col1",
                DataType.INT,
                "col2",
                DataType.TEXT,
                "col3",
                DataType.BIGINT,
                "col4",
                DataType.FLOAT,
                "col5",
                DataType.DOUBLE),
            ImmutableSet.of("col1"),
            ImmutableSet.of("col2", "col3", "col4"),
            ImmutableMap.of(
                "col2",
                ClusteringOrder.ASC,
                "col3",
                ClusteringOrder.DESC,
                "col4",
                ClusteringOrder.ASC),
            ImmutableSet.of("col5"),
            ImmutableMap.of("name1", "val1", "name2", "val2", "name3", "val3"));

    CreateTableStatement statement2 =
        CreateTableStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            true,
            ImmutableMap.of(
                "col1",
                DataType.INT,
                "col2",
                DataType.TEXT,
                "col3",
                DataType.BIGINT,
                "col4",
                DataType.FLOAT,
                "col5",
                DataType.DOUBLE),
            ImmutableSet.of("col1"),
            ImmutableSet.of("col2", "col3", "col4"),
            ImmutableMap.of("col2", ClusteringOrder.ASC, "col3", ClusteringOrder.DESC),
            ImmutableSet.of("col5"),
            ImmutableMap.of("name1", "val1", "name2", "val2", "name3", "val3"));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement1)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement2)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_CreateTableStatementWithInvalidClusteringOrdersGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    CreateTableStatement statement1 =
        CreateTableStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            true,
            ImmutableMap.of(
                "col1",
                DataType.INT,
                "col2",
                DataType.TEXT,
                "col3",
                DataType.BIGINT,
                "col4",
                DataType.FLOAT,
                "col5",
                DataType.DOUBLE),
            ImmutableSet.of("col1"),
            ImmutableSet.of("col2", "col3", "col4"),
            ImmutableMap.of("col3", ClusteringOrder.DESC, "col4", ClusteringOrder.ASC),
            ImmutableSet.of("col3"),
            ImmutableMap.of("name1", "val1", "name2", "val2", "name3", "val3"));

    CreateTableStatement statement2 =
        CreateTableStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            true,
            ImmutableMap.of(
                "col1",
                DataType.INT,
                "col2",
                DataType.TEXT,
                "col3",
                DataType.BIGINT,
                "col4",
                DataType.FLOAT,
                "col5",
                DataType.DOUBLE),
            ImmutableSet.of("col1"),
            ImmutableSet.of("col2", "col3", "col4"),
            ImmutableMap.of(
                "col3",
                ClusteringOrder.DESC,
                "col4",
                ClusteringOrder.ASC,
                "col2",
                ClusteringOrder.ASC),
            ImmutableSet.of("col3"),
            ImmutableMap.of("name1", "val1", "name2", "val2", "name3", "val3"));

    CreateTableStatement statement3 =
        CreateTableStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            true,
            ImmutableMap.of(
                "col1",
                DataType.INT,
                "col2",
                DataType.TEXT,
                "col3",
                DataType.BIGINT,
                "col4",
                DataType.FLOAT,
                "col5",
                DataType.DOUBLE),
            ImmutableSet.of("col1"),
            ImmutableSet.of("col2", "col3", "col4"),
            ImmutableMap.of(
                "col1",
                ClusteringOrder.ASC,
                "col2",
                ClusteringOrder.DESC,
                "col3",
                ClusteringOrder.ASC),
            ImmutableSet.of("col3"),
            ImmutableMap.of("name1", "val1", "name2", "val2", "name3", "val3"));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_ProperSelectStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    SelectStatement statement1 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col1")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement2 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement3 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThanOrEqualTo(Value.ofText("ddd")),
                Predicate.column("c2").isLessThan(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement4 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c1").isLessThanOrEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement5 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThan(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement6 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThanOrEqualTo(Value.ofText("ccc"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement7 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isLessThanOrEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement8 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1").as("a"),
                Projection.column("p2").as("b"),
                Projection.column("c1").as("c"),
                Projection.column("c2").as("d"),
                Projection.column("col2").as("e")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isLessThan(Value.ofText("ccc"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement1)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement2)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement3)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement4)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement5)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement6)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement7)).doesNotThrowAnyException();
    assertThatCode(() -> statementValidator.validate(statement8)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_SelectStatementWithDuplicatedProjectedColumnsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col1")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_SelectStatementWithNonPrimaryKeyColumnInPredicatesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd")),
                Predicate.column("co2").isEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_SelectStatementWithDuplicatePartitionKeyColumnsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("p2").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_SelectStatementWithSpecifyingPartitionKeyColumnsWithNonIsEqualToPredicateGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_SelectStatementWithNullPartitionKeyColumnGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofNull()),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_SelectStatementWithInvalidClusteringKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement1 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement2 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ddd")),
                Predicate.column("c1").isEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement3 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement4 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c1").isGreaterThanOrEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement5 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isLessThanOrEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isLessThan(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement6 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c1").isLessThan(Value.ofText("ddd")),
                Predicate.column("c2").isEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement7 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThanOrEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement8 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThanOrEqualTo(Value.ofText("ddd")),
                Predicate.column("c2").isGreaterThan(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement9 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isLessThan(Value.ofText("ddd")),
                Predicate.column("c2").isLessThanOrEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement10 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd")),
                Predicate.column("c2").isEqualTo(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement11 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd")),
                Predicate.column("c2").isEqualTo(Value.ofText("eee")),
                Predicate.column("c2").isEqualTo(Value.ofText("fff"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement3))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement4))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement5))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement6))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement7))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement8))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement9))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement10))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement11))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_ProperSelectStatementForIndexScanGiven_ShouldNotThrowAnyException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("col2"),
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2")),
            ImmutableList.of(Predicate.column("col2").isEqualTo(Value.ofText("aaa"))),
            ImmutableList.of(),
            Value.ofInt(100));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_SelectStatementForIndexScanWithClusteringOrderingGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("col2"),
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2")),
            ImmutableList.of(Predicate.column("col2").isEqualTo(Value.ofText("aaa"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_ProperInsertStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("p1").value(Value.ofText("aaa")),
                Assignment.column("p2").value(Value.ofText("bbb")),
                Assignment.column("c1").value(Value.ofText("ccc")),
                Assignment.column("c2").value(Value.ofText("ddd")),
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_InsertStatementWithDuplicateAssignmentsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("p1").value(Value.ofText("aaa")),
                Assignment.column("p1").value(Value.ofText("aaa")),
                Assignment.column("p2").value(Value.ofText("bbb")),
                Assignment.column("c1").value(Value.ofText("ccc")),
                Assignment.column("c2").value(Value.ofText("ddd")),
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_InsertStatementWithLackOfPrimaryKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("p1").value(Value.ofText("aaa")),
                Assignment.column("c1").value(Value.ofText("ccc")),
                Assignment.column("c2").value(Value.ofText("ddd")),
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_InsertStatementWithNullPrimaryKeyColumnGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("p1").value(Value.ofText("aaa")),
                Assignment.column("p2").value(Value.ofText("bbb")),
                Assignment.column("c1").value(Value.ofNull()),
                Assignment.column("c2").value(Value.ofText("ddd")),
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_ProperUpdateStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_UpdateStatementWithNonPrimaryKeyColumnsInPredicateGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd")),
                Predicate.column("col2").isEqualTo(Value.ofText("fff"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_UpdateStatementWithDuplicatePrimaryKeyColumnsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_UpdateStatementWithSpecifyingPrimaryKeyColumnsWithNonIsEqualToPredicateGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThan(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_UpdateStatementWithNullPrimaryKeyColumnGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("eee")),
                Assignment.column("col2").value(Value.ofText("fff"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofNull()),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_ProperDeleteStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatCode(() -> statementValidator.validate(statement)).doesNotThrowAnyException();
  }

  @Test
  public void
      validate_DeleteStatementWithNonPrimaryKeyColumnsInPredicateGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd")),
                Predicate.column("col2").isEqualTo(Value.ofText("fff"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_DeleteStatementWithDuplicatePrimaryKeyColumnsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_DeleteStatementWithSpecifyingPrimaryKeyColumnsWithNonIsEqualToPredicateGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThan(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      validate_DeleteStatementWithNullPrimaryKeyColumnGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofNull()),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_InsertStatementWithBindMarkerGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("p1").value(BindMarker.positional()),
                Assignment.column("p2").value(BindMarker.positional()),
                Assignment.column("c1").value(BindMarker.named("name1")),
                Assignment.column("c2").value(BindMarker.named("name2")),
                Assignment.column("col1").value(BindMarker.positional()),
                Assignment.column("col2").value(BindMarker.positional())));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_UpdateStatementWithBindMarkerGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement1 =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.positional()),
                Assignment.column("col2").value(BindMarker.named("name1"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))));

    UpdateStatement statement2 =
        UpdateStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Assignment.column("col1").value(Value.ofText("aaa")),
                Assignment.column("col2").value(Value.ofText("bbb"))),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(BindMarker.positional()),
                Predicate.column("p2").isEqualTo(BindMarker.positional()),
                Predicate.column("c1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("c2").isEqualTo(BindMarker.named("name2"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_DeleteStatementWithBindMarkerGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(BindMarker.positional()),
                Predicate.column("p2").isEqualTo(BindMarker.positional()),
                Predicate.column("c1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("c2").isEqualTo(BindMarker.named("name2"))));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_SelectStatementWithBindMarkerGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement1 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col1")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(BindMarker.positional()),
                Predicate.column("p2").isEqualTo(BindMarker.positional()),
                Predicate.column("c1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("c2").isEqualTo(BindMarker.named("name2"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofInt(100));

    SelectStatement statement2 =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col1")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            BindMarker.positional());

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> statementValidator.validate(statement2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validate_SelectStatementWithNonIntTypeLimit_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            NAMESPACE_NAME,
            TABLE_NAME,
            ImmutableList.of(
                Projection.column("p1"),
                Projection.column("p2"),
                Projection.column("c1"),
                Projection.column("c2"),
                Projection.column("col1")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            Value.ofText("limit"));

    // Act Assert
    assertThatThrownBy(() -> statementValidator.validate(statement))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
