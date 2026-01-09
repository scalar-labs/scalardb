package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.VirtualTableInfoManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcDatabaseTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";

  @Mock private DatabaseConfig databaseConfig;
  @Mock private HikariDataSource dataSource;
  @Mock private HikariDataSource tableMetadataDataSource;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private VirtualTableInfoManager virtualTableInfoManager;
  @Mock private JdbcService jdbcService;

  @Mock private ResultInterpreter resultInterpreter;
  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JdbcDatabase jdbcDatabase;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDatabase =
        new JdbcDatabase(
            databaseConfig,
            RdbEngine.createRdbEngineStrategy(RdbEngine.POSTGRESQL),
            dataSource,
            tableMetadataDataSource,
            tableMetadataManager,
            virtualTableInfoManager,
            jdbcService,
            false);
  }

  @Test
  public void whenGetOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange

    // Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    jdbcDatabase.get(get);

    // Assert
    verify(connection).setReadOnly(true);
    verify(jdbcService).get(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenGetOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.get(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Get get =
                  Get.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val"))
                      .build();
              jdbcDatabase.get(get);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setReadOnly(true);
    verify(connection).close();
  }

  @Test
  public void whenScanOperationExecutedAndScannerClosed_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any()))
        .thenReturn(
            new ScannerImpl(resultInterpreter, connection, preparedStatement, resultSet, true));

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = jdbcDatabase.scan(scan);
    scanner.close();

    // Assert
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(jdbcService).getScanner(any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan =
                  Scan.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val"))
                      .build();
              jdbcDatabase.scan(scan);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJdbcServiceThrowsIllegalArgumentException_shouldCloseConnectionAndThrowIllegalArgumentException()
          throws Exception {
    // Arrange
    Exception cause = new IllegalArgumentException("Table not found");
    // Simulate the table not found scenario.
    when(jdbcService.getScanner(any(), any())).thenThrow(cause);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan =
                  Scan.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val"))
                      .build();
              jdbcDatabase.scan(scan);
            })
        .isInstanceOf(IllegalArgumentException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndScannerClosed_SQLExceptionThrownByConnectionCommit_shouldThrowIOException()
          throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any()))
        .thenReturn(
            new ScannerImpl(resultInterpreter, connection, preparedStatement, resultSet, true));
    doThrow(sqlException).when(connection).commit();

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = jdbcDatabase.scan(scan);
    assertThatThrownBy(scanner::close).isInstanceOf(IOException.class).hasCause(sqlException);

    // Assert
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(jdbcService).getScanner(any(), any());
    verify(connection).commit();
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void whenPutOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    jdbcDatabase.put(put);

    // Assert
    verify(jdbcService).put(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  Put.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .textValue("v1", "val2")
                      .condition(ConditionBuilder.putIfNotExists())
                      .build();
              jdbcDatabase.put(put);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  Put.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .textValue("v1", "val2")
                      .build();
              jdbcDatabase.put(put);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).close();
  }

  @Test
  public void whenDeleteOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(true);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .build();
    jdbcDatabase.delete(delete);

    // Assert
    verify(jdbcService).delete(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  Delete.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .condition(ConditionBuilder.deleteIfExists())
                      .build();
              jdbcDatabase.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  Delete.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .build();
              jdbcDatabase.delete(delete);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).close();
  }

  @Test
  public void whenMutateOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenReturn(true);

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
    jdbcDatabase.mutate(Arrays.asList(put, delete));

    // Assert
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  Put.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .textValue("v1", "val2")
                      .condition(ConditionBuilder.putIfNotExists())
                      .build();
              Delete delete =
                  Delete.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .condition(ConditionBuilder.deleteIfExists())
                      .build();
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
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
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void mutate_withConflictError_shouldThrowRetriableExecutionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenThrow(sqlException);
    when(sqlException.getSQLState()).thenReturn("40001");

    // Act Assert
    assertThatThrownBy(
            () -> {
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
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void mutate_WhenSettingAutoCommitFails_ShouldThrowExceptionAndCloseConnection()
      throws SQLException, ExecutionException {
    // Arrange
    Exception exception = new RuntimeException("Failed to set auto-commit");
    doThrow(exception).when(connection).setAutoCommit(anyBoolean());

    // Act Assert
    assertThatThrownBy(
            () -> {
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
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isEqualTo(exception);
    verify(connection).setAutoCommit(false);
    verify(jdbcService, never()).mutate(any(), any());
    verify(connection, never()).rollback();
    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithInnerJoin_ShouldDivideIntoSourceTablesAndCallJdbcServiceWithBothPuts()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .intValue("left_col2", 100)
            .textValue("right_col1", "right_val1")
            .intValue("right_col2", 200)
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .intValue("left_col2", 100)
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .intValue("right_col2", 200)
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithPutIfExistsAndInnerJoin_ShouldApplyConditionToBothSourceTables()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithPutIfExistsAndLeftOuterJoin_ShouldApplyConditionToLeftSourceTableOnly()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void put_ForVirtualTableWithPutIf_ShouldDivideConditionsBasedOnColumns() throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isEqualToText("check_val2"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("right_col1").isEqualToText("check_val2"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithLeftOuterJoinAndAllIsNullConditionsOnRightTable_ShouldUsePutIfNotExists()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .condition(ConditionBuilder.putIfNotExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithLeftOuterJoinAndMixedConditionsOnRightTable_ShouldUsePutIfWithAllConditions()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isEqualToInt(123))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isEqualToInt(123))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      put_ForVirtualTableWithLeftOuterJoinAndAllIsNullConditionsOnRightTableWithDisabledConversion_ShouldUsePutIfWithAllConditions()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
        attributes, false);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    // Act
    jdbcDatabase.put(put);

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void
      delete_ForVirtualTableWithDeleteIfExistsAndInnerJoin_ShouldApplyConditionToBothSourceTables()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.delete(delete);

    // Assert
    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(1)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void
      delete_ForVirtualTableWithDeleteIfExistsAndLeftOuterJoin_ShouldApplyConditionToLeftSourceTableOnly()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.delete(delete);

    // Assert
    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(ConditionBuilder.deleteIfExists())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(1)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void delete_ForVirtualTableWithDeleteIf_ShouldDivideConditionsBasedOnColumns()
      throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isEqualToText("check_val2"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.delete(delete);

    // Assert
    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("right_col1").isEqualToText("check_val2"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(1)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void
      delete_ForVirtualTableWithLeftOuterJoinAndAllIsNullConditionsOnRightTable_ShouldThrowException()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act & Assert
    assertThatThrownBy(() -> jdbcDatabase.delete(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      delete_ForVirtualTableWithLeftOuterJoinAndMixedConditionsOnRightTable_ShouldUseDeleteIfWithAllConditions()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isEqualToInt(123))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.delete(delete);

    // Assert
    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isEqualToInt(123))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(1)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void
      delete_ForVirtualTableWithLeftOuterJoinAndAllIsNullConditionsOnRightTableWithAllowedAttribute_ShouldDeleteProperly()
          throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.LEFT_OUTER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
        attributes, true);

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .and(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    // Act
    jdbcDatabase.delete(delete);

    // Assert
    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column("left_col1").isEqualToText("check_val"))
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("right_col1").isNullText())
                    .and(ConditionBuilder.column("right_col2").isNullInt())
                    .build())
            .consistency(Consistency.LINEARIZABLE)
            .attributes(attributes)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    // For LEFT_OUTER join with IS_NULL conditions allowed, both deletes should be created
    assertThat(mutations).hasSize(2);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(1)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void mutate_ForVirtualTableWithPutAndDelete_ShouldDivideIntoSourceTables()
      throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val2"))
            .clusteringKey(Key.ofText("ck1", "ck_val2"))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.mutate(Arrays.asList(put, delete));

    // Assert
    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedLeftDelete =
        Delete.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val2"))
            .clusteringKey(Key.ofText("ck1", "ck_val2"))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Delete expectedRightDelete =
        Delete.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val2"))
            .clusteringKey(Key.ofText("ck1", "ck_val2"))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    // 2 Puts (from Put divided) + 2 Deletes (from Delete divided) = 4 mutations
    assertThat(mutations).hasSize(4);
    assertThat(mutations.get(0)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(1)).isEqualTo(expectedRightPut);
    assertThat(mutations.get(2)).isEqualTo(expectedLeftDelete);
    assertThat(mutations.get(3)).isEqualTo(expectedRightDelete);

    verify(connection).close();
  }

  @Test
  public void mutate_ForMixOfRegularTableAndVirtualTable_ShouldHandleBothCorrectly()
      throws Exception {
    // Arrange
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo(VirtualTableJoinType.INNER);
    when(virtualTableInfoManager.getVirtualTableInfo(NAMESPACE, TABLE))
        .thenReturn(virtualTableInfo);
    when(virtualTableInfoManager.getVirtualTableInfo("regular_ns", "regular_table"))
        .thenReturn(null);
    when(tableMetadataManager.getTableMetadata("left_ns", "left_table"))
        .thenReturn(createLeftSourceTableMetadata());
    when(tableMetadataManager.getTableMetadata("right_ns", "right_table"))
        .thenReturn(createRightSourceTableMetadata());
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    // Regular table put
    Put regularPut =
        Put.newBuilder()
            .namespace("regular_ns")
            .table("regular_table")
            .partitionKey(Key.ofText("pk1", "regular_val"))
            .clusteringKey(Key.ofText("ck1", "regular_ck"))
            .textValue("col1", "value1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Virtual table put
    Put virtualPut =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .textValue("right_col1", "right_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    jdbcDatabase.mutate(Arrays.asList(regularPut, virtualPut));

    // Assert
    Put expectedRegularPut =
        Put.newBuilder()
            .namespace("regular_ns")
            .table("regular_table")
            .partitionKey(Key.ofText("pk1", "regular_val"))
            .clusteringKey(Key.ofText("ck1", "regular_ck"))
            .textValue("col1", "value1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedLeftPut =
        Put.newBuilder()
            .namespace("left_ns")
            .table("left_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("left_col1", "left_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    Put expectedRightPut =
        Put.newBuilder()
            .namespace("right_ns")
            .table("right_table")
            .partitionKey(Key.ofText("pk1", "val1"))
            .clusteringKey(Key.ofText("ck1", "ck_val1"))
            .textValue("right_col1", "right_val1")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Mutation>> mutationsCaptor = ArgumentCaptor.forClass(List.class);
    verify(jdbcService).mutate(mutationsCaptor.capture(), any());

    List<Mutation> mutations = mutationsCaptor.getValue();
    // 1 regular Put + 2 virtual Puts (from virtual Put divided) = 3 mutations
    assertThat(mutations).hasSize(3);
    assertThat(mutations.get(0)).isEqualTo(expectedRegularPut);
    assertThat(mutations.get(1)).isEqualTo(expectedLeftPut);
    assertThat(mutations.get(2)).isEqualTo(expectedRightPut);

    verify(connection).close();
  }

  @Test
  public void get_WhenRequiresExplicitCommitIsTrue_ShouldSetAutoCommitFalseAndCommitAfterExecution()
      throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();

    // Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    database.get(get);

    // Assert
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(jdbcService).get(any(), any());
    verify(connection).commit();
    verify(connection, never()).rollback();
    verify(connection).close();
  }

  @Test
  public void
      get_WhenRequiresExplicitCommitIsTrueAndJdbcServiceThrowsSQLException_ShouldRollbackAndThrowExecutionException()
          throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();
    when(jdbcService.get(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Get get =
                  Get.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val"))
                      .build();
              database.get(get);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection, never()).commit();
    verify(connection).close();
  }

  @Test
  public void put_WhenRequiresExplicitCommitIsTrue_ShouldSetAutoCommitFalseAndCommitAfterExecution()
      throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();
    when(jdbcService.put(any(), any())).thenReturn(true);

    // Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .textValue("v1", "val2")
            .build();
    database.put(put);

    // Assert
    verify(connection).setAutoCommit(false);
    verify(jdbcService).put(any(), any());
    verify(connection).commit();
    verify(connection, never()).rollback();
    verify(connection).close();
  }

  @Test
  public void
      put_WhenRequiresExplicitCommitIsTrueAndJdbcServiceThrowsSQLException_ShouldRollbackAndThrowExecutionException()
          throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();
    when(jdbcService.put(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  Put.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .textValue("v1", "val2")
                      .build();
              database.put(put);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection, never()).commit();
    verify(connection).close();
  }

  @Test
  public void
      delete_WhenRequiresExplicitCommitIsTrue_ShouldSetAutoCommitFalseAndCommitAfterExecution()
          throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();
    when(jdbcService.delete(any(), any())).thenReturn(true);

    // Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val1"))
            .build();
    database.delete(delete);

    // Assert
    verify(connection).setAutoCommit(false);
    verify(jdbcService).delete(any(), any());
    verify(connection).commit();
    verify(connection, never()).rollback();
    verify(connection).close();
  }

  @Test
  public void
      delete_WhenRequiresExplicitCommitIsTrueAndJdbcServiceThrowsSQLException_ShouldRollbackAndThrowExecutionException()
          throws Exception {
    // Arrange
    JdbcDatabase database = createJdbcDatabaseWithExplicitCommit();
    when(jdbcService.delete(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  Delete.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLE)
                      .partitionKey(Key.ofText("p1", "val1"))
                      .build();
              database.delete(delete);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection, never()).commit();
    verify(connection).close();
  }

  private VirtualTableInfo createVirtualTableInfo(VirtualTableJoinType joinType) {
    return new VirtualTableInfo() {
      @Override
      public String getNamespaceName() {
        return NAMESPACE;
      }

      @Override
      public String getTableName() {
        return TABLE;
      }

      @Override
      public String getLeftSourceNamespaceName() {
        return "left_ns";
      }

      @Override
      public String getLeftSourceTableName() {
        return "left_table";
      }

      @Override
      public String getRightSourceNamespaceName() {
        return "right_ns";
      }

      @Override
      public String getRightSourceTableName() {
        return "right_table";
      }

      @Override
      public VirtualTableJoinType getJoinType() {
        return joinType;
      }
    };
  }

  private TableMetadata createLeftSourceTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.TEXT)
        .addColumn("ck1", DataType.TEXT)
        .addColumn("ck2", DataType.INT)
        .addColumn("left_col1", DataType.TEXT)
        .addColumn("left_col2", DataType.INT)
        .addColumn("left_col3", DataType.BIGINT)
        .addPartitionKey("pk1")
        .addClusteringKey("ck1")
        .addClusteringKey("ck2")
        .build();
  }

  private TableMetadata createRightSourceTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.TEXT)
        .addColumn("ck1", DataType.TEXT)
        .addColumn("ck2", DataType.INT)
        .addColumn("right_col1", DataType.TEXT)
        .addColumn("right_col2", DataType.INT)
        .addColumn("right_col3", DataType.DOUBLE)
        .addPartitionKey("pk1")
        .addClusteringKey("ck1")
        .addClusteringKey("ck2")
        .build();
  }

  private JdbcDatabase createJdbcDatabaseWithExplicitCommit() {
    return new JdbcDatabase(
        databaseConfig,
        RdbEngine.createRdbEngineStrategy(RdbEngine.POSTGRESQL),
        dataSource,
        tableMetadataDataSource,
        tableMetadataManager,
        virtualTableInfoManager,
        jdbcService,
        true);
  }
}
