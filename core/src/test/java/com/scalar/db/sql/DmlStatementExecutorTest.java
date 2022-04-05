package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DmlStatementExecutorTest {

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

  @Mock private TransactionCrudOperable transaction;
  @Mock private TableMetadataManager tableMetadataManager;

  private DmlStatementExecutor dmlStatementExecutor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(tableMetadataManager.getTableMetadata(NAMESPACE_NAME, TABLE_NAME))
        .thenReturn(TABLE_METADATA);

    dmlStatementExecutor = new DmlStatementExecutor(tableMetadataManager);
  }

  @Test
  public void execute_SelectStatementWithFullPrimaryKeyGiven_ShouldGetProperly()
      throws CrudException {
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
            100);

    Result result = mock(Result.class);
    when(transaction.get(any())).thenReturn(Optional.of(result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .get(
            new Get(Key.of("p1", "aaa", "p2", "bbb"), Key.of("c1", "ccc", "c2", "ddd"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col1"))
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithPartialClusteringKeyGiven_ShouldScanProperly()
      throws CrudException {
    // Arrange
    SelectStatement statement =
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
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withStart(Key.ofText("c1", "ccc"))
                .withEnd(Key.ofText("c1", "ccc"))
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithFullClusteringKeyRangeGiven_ShouldScanProperly()
      throws CrudException {
    // Arrange
    SelectStatement statement =
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
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThanOrEqualTo(Value.ofText("ddd")),
                Predicate.column("c2").isLessThan(Value.ofText("eee"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withStart(Key.of("c1", "ccc", "c2", "ddd"))
                .withEnd(Key.of("c1", "ccc", "c2", "eee"), false)
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithPartialClusteringKeyRangeGiven_ShouldScanProperly()
      throws CrudException {
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
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThan(Value.ofText("ccc")),
                Predicate.column("c1").isLessThanOrEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withStart(Key.ofText("c1", "ccc"), false)
                .withEnd(Key.ofText("c1", "ddd"))
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithFullStartClusteringKeyGiven_ShouldScanProperly()
      throws CrudException {
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
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isGreaterThan(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withStart(Key.of("c1", "ccc", "c2", "ddd"), false)
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithPartialStartClusteringKeyGiven_ShouldScanProperly()
      throws CrudException {
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
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isGreaterThanOrEqualTo(Value.ofText("ccc"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withStart(Key.ofText("c1", "ccc"))
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithFullEndClusteringKeyGiven_ShouldScanProperly()
      throws CrudException {
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
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isEqualTo(Value.ofText("ccc")),
                Predicate.column("c2").isLessThanOrEqualTo(Value.ofText("ddd"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withEnd(Key.of("c1", "ccc", "c2", "ddd"))
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementWithPartialEndClusteringKeyGiven_ShouldScanProperly()
      throws CrudException {
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
                Projection.column("col2")),
            ImmutableList.of(
                Predicate.column("p1").isEqualTo(Value.ofText("aaa")),
                Predicate.column("p2").isEqualTo(Value.ofText("bbb")),
                Predicate.column("c1").isLessThan(Value.ofText("ccc"))),
            ImmutableList.of(
                ClusteringOrdering.column("c1").asc(), ClusteringOrdering.column("c2").desc()),
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.of("p1", "aaa", "p2", "bbb"))
                .withProjections(Arrays.asList("p1", "p2", "c1", "c2", "col2"))
                .withEnd(Key.ofText("c1", "ccc"), false)
                .withOrdering(Ordering.asc("c1"))
                .withOrdering(Ordering.desc("c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_SelectStatementForIndexScanGiven_ShouldScanProperly() throws CrudException {
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
            100);

    Result result = mock(Result.class);
    when(transaction.scan(any())).thenReturn(Arrays.asList(result, result, result));

    // Act
    dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .scan(
            new Scan(Key.ofText("col2", "aaa"))
                .withProjections(Arrays.asList("col2", "p1", "p2", "c1", "c2"))
                .withLimit(100)
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
  }

  @Test
  public void execute_InsertStatementGiven_ShouldPutProperly() throws CrudException {
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

    // Act
    ResultSet resultSet = dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .put(
            new Put(Key.of("p1", "aaa", "p2", "bbb"), Key.of("c1", "ccc", "c2", "ddd"))
                .withTextValue("col1", "eee")
                .withTextValue("col2", "fff")
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
    assertThat(resultSet).isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_UpdateStatementGiven_ShouldPutProperly() throws CrudException {
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

    // Act
    ResultSet resultSet = dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .put(
            new Put(Key.of("p1", "aaa", "p2", "bbb"), Key.of("c1", "ccc", "c2", "ddd"))
                .withTextValue("col1", "eee")
                .withTextValue("col2", "fff")
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
    assertThat(resultSet).isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_DeleteStatementGiven_ShouldDeleteProperly() throws CrudException {
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

    // Act
    ResultSet resultSet = dmlStatementExecutor.execute(transaction, statement);

    // Assert
    verify(transaction)
        .delete(
            new Delete(Key.of("p1", "aaa", "p2", "bbb"), Key.of("c1", "ccc", "c2", "ddd"))
                .forNamespace(NAMESPACE_NAME)
                .forTable(TABLE_NAME));
    assertThat(resultSet).isEqualTo(EmptyResultSet.INSTANCE);
  }
}
