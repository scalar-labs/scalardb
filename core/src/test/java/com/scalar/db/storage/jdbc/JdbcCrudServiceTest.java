package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpdateQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcCrudServiceTest {

  private static final int SCAN_FETCH_SIZE = 10;
  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";

  @Mock private QueryBuilder queryBuilder;
  @Mock private OperationChecker operationChecker;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private RdbEngineStrategy rdbEngine;

  @Mock private SelectQuery.Builder selectQueryBuilder;
  @Mock private SelectQuery selectQuery;
  @Mock private UpsertQuery.Builder upsertQueryBuilder;
  @Mock private UpsertQuery upsertQuery;
  @Mock private DeleteQuery.Builder deleteQueryBuilder;
  @Mock private DeleteQuery deleteQuery;
  @Mock private UpdateQuery.Builder updateQueryBuilder;
  @Mock private UpdateQuery updateQuery;
  @Mock private InsertQuery.Builder insertQueryBuilder;
  @Mock private InsertQuery insertQuery;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Connection connection;

  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JdbcCrudService jdbcCrudService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    jdbcCrudService =
        new JdbcCrudService(
            tableMetadataManager, operationChecker, rdbEngine, queryBuilder, SCAN_FETCH_SIZE);

    // Arrange
    when(tableMetadataManager.getTableMetadata(any(Operation.class)))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn("p1", DataType.TEXT)
                .addColumn("v1", DataType.TEXT)
                .addPartitionKey("p1")
                .build());
    when(connection.getMetaData().getURL()).thenReturn("jdbc:mysql://localhost:3306/");
  }

  @Test
  public void whenGetOperationExecuted_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any(), anySet())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    jdbcCrudService.get(get, connection);

    // Assert
    verify(operationChecker).check(any(Get.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenGetScannerExecuted_withScan_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any(), anyBoolean(), any(), anyBoolean(), anySet()))
        .thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = jdbcCrudService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
    verify(preparedStatement).setFetchSize(SCAN_FETCH_SIZE);

    assertThat(scanner).isNotNull();
    assertThat(scanner).isInstanceOf(ScannerImpl.class);
  }

  @Test
  public void whenGetScannerExecuted_withScanAll_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan = Scan.newBuilder().namespace(NAMESPACE).table(TABLE).all().build();
    Scanner scanner = jdbcCrudService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
    verify(preparedStatement).setFetchSize(SCAN_FETCH_SIZE);

    assertThat(scanner).isNotNull();
    assertThat(scanner).isInstanceOf(ScannerImpl.class);
  }

  @Test
  public void whenGetScannerExecuted_withCrossPartitionScan_shouldCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(anySet())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .all()
            .where(ConditionBuilder.column("column").isEqualToInt(10))
            .build();
    Scanner scanner = jdbcCrudService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
    verify(queryBuilder.select(any())).from(any(), any(), any());
    verify(queryBuilder.select(any())).where(anySet());
    verify(queryBuilder.select(any())).orderBy(anyList());
    verify(queryBuilder.select(any())).limit(anyInt());
    verify(preparedStatement).setFetchSize(SCAN_FETCH_SIZE);

    assertThat(scanner).isNotNull();
    assertThat(scanner).isInstanceOf(ScannerImpl.class);
  }

  @Test
  public void whenScanExecuted_withScan_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any(), anyBoolean(), any(), anyBoolean(), anySet()))
        .thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    jdbcCrudService.scan(scan, connection);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenScanExecuted_withScanAll_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scanAll = Scan.newBuilder().namespace(NAMESPACE).table(TABLE).all().build();
    jdbcCrudService.scan(scanAll, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenScanExecuted_withCrossPartitionScan_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.from(any(), any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .all()
            .where(ConditionBuilder.column("p1").isLessThanText("val"))
            .build();
    jdbcCrudService.scan(scan, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
    verify(selectQueryBuilder).from(any(), any(), any());
    verify(selectQueryBuilder).where(any());
    verify(selectQueryBuilder).orderBy(any());
    verify(selectQueryBuilder).limit(anyInt());
  }

  @Test
  public void whenPutOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.upsertInto(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).upsertInto(any(), any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("v1").isEqualToText("val2")).build())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("v1").isEqualToText("val2")).build())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfExists())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfExists())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any(), any());
  }

  @Test
  public void
      whenPutOperationWithPutIfNotExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.insertInto(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).insertInto(any(), any(), any());
  }

  @Test
  public void
      whenPutOperationWithPutIfNotExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.insertInto(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenThrow(sqlException);
    when(rdbEngine.isDuplicateKeyError(sqlException)).thenReturn(true);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    boolean ret = jdbcCrudService.put(put, connection);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).insertInto(any(), any(), any());
  }

  @Test
  public void whenDeleteOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .build();
    boolean ret = jdbcCrudService.delete(delete, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("v1").isEqualToText("val2"))
                    .build())
            .build();
    boolean ret = jdbcCrudService.delete(delete, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("v1").isEqualToText("val2"))
                    .build())
            .build();
    boolean ret = jdbcCrudService.delete(delete, connection);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .build();
    boolean ret = jdbcCrudService.delete(delete, connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .build();
    boolean ret = jdbcCrudService.delete(delete, connection);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void whenMutateOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);

    when(queryBuilder.upsertInto(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.sql()).thenReturn("UPSERT_SQL");

    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.sql()).thenReturn("DELETE_SQL");

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(put, delete), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(anyList());
    verify(queryBuilder).upsertInto(any(), any(), any());
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }

  @Test
  public void mutate_withMultiplePutsHavingSameSql_shouldExecuteBatch() throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1, 1});

    when(queryBuilder.upsertInto(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.sql()).thenReturn("UPSERT_SQL");

    // Act
    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val3"))
            .textValue("v1", "val4")
            .build();
    Put put3 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val5"))
            .textValue("v1", "val6")
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(put1, put2, put3), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(preparedStatement, times(3)).addBatch();
    verify(preparedStatement).executeBatch();
    verify(preparedStatement, never()).executeUpdate();
  }

  @Test
  public void mutate_withMultipleDeletesHavingSameSql_shouldExecuteBatch() throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.sql()).thenReturn("DELETE_SQL");

    // Act
    Delete delete1 =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .build();
    Delete delete2 =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val2"))
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(delete1, delete2), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(preparedStatement, times(2)).addBatch();
    verify(preparedStatement).executeBatch();
    verify(preparedStatement, never()).executeUpdate();
  }

  @Test
  public void mutate_withPutIfNotExists_shouldExecuteIndividually() throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    when(queryBuilder.insertInto(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(insertQuery.sql()).thenReturn("INSERT_SQL");

    // Act
    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val3"))
            .textValue("v1", "val4")
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(put1, put2), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(preparedStatement, never()).addBatch();
    verify(preparedStatement, never()).executeBatch();
    verify(preparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void mutate_withConditionalMutationsBatch_whenConditionNotMet_shouldReturnFalse()
      throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 0}); // Second mutation fails

    when(queryBuilder.update(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.sql()).thenReturn("UPDATE_SQL");

    // Act
    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .condition(ConditionBuilder.putIfExists())
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val3"))
            .textValue("v1", "val4")
            .condition(ConditionBuilder.putIfExists())
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(put1, put2), connection);

    // Assert
    assertThat(ret).isFalse();
    verify(preparedStatement, times(2)).addBatch();
    verify(preparedStatement).executeBatch();
  }

  @Test
  public void mutate_withDifferentSqls_shouldExecuteInSeparateBatches() throws Exception {
    // Arrange
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    when(queryBuilder.upsertInto(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.sql()).thenReturn("UPSERT_SQL");

    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.sql()).thenReturn("DELETE_SQL");

    // Act
    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val3"))
            .textValue("v1", "val4")
            .build();
    Delete delete1 =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val5"))
            .build();
    Delete delete2 =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val6"))
            .build();
    boolean ret = jdbcCrudService.mutate(Arrays.asList(put1, put2, delete1, delete2), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(preparedStatement, times(4)).addBatch();
    verify(preparedStatement, times(2)).executeBatch(); // Two separate batches
  }
}
