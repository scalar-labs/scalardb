package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
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

public class JdbcServiceTest {

  private static final int SCANNER_FETCH_SIZE = 10;
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

  private JdbcService jdbcService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    jdbcService =
        new JdbcService(
            tableMetadataManager, operationChecker, rdbEngine, queryBuilder, SCANNER_FETCH_SIZE);

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
    Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    jdbcService.get(get, connection);

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
    Scan scan = new Scan(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    Scanner scanner = jdbcService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
    verify(preparedStatement).setFetchSize(SCANNER_FETCH_SIZE);

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
    Scan scan = new ScanAll().forNamespace(NAMESPACE).forTable(TABLE);
    Scanner scanner = jdbcService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
    verify(preparedStatement).setFetchSize(SCANNER_FETCH_SIZE);

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
    Scanner scanner = jdbcService.getScanner(scan, connection);

    // Assert
    verify(operationChecker).check(any(ScanAll.class));
    verify(queryBuilder).select(any());
    verify(queryBuilder.select(any())).from(any(), any(), any());
    verify(queryBuilder.select(any())).where(anySet());
    verify(queryBuilder.select(any())).orderBy(anyList());
    verify(queryBuilder.select(any())).limit(anyInt());
    verify(preparedStatement).setFetchSize(SCANNER_FETCH_SIZE);

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
    Scan scan = new Scan(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    jdbcService.scan(scan, connection);

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
    ScanAll scanAll = new ScanAll().forNamespace(NAMESPACE).forTable(TABLE);
    jdbcService.scan(scanAll, connection);

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
    jdbcService.scan(scan, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)))
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)))
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(new PutIfExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(new PutIfExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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

    // Act
    Put put =
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(new PutIfNotExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(new PutIfNotExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.put(put, connection);

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
    Delete delete = new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
    boolean ret = jdbcService.delete(delete, connection);

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
        new Delete(new Key("p1", "val1"))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)))
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.delete(delete, connection);

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
        new Delete(new Key("p1", "val1"))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)))
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.delete(delete, connection);

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
        new Delete(new Key("p1", "val1"))
            .withCondition(new DeleteIfExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.delete(delete, connection);

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
        new Delete(new Key("p1", "val1"))
            .withCondition(new DeleteIfExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    boolean ret = jdbcService.delete(delete, connection);

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

    when(queryBuilder.deleteFrom(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);

    // Act
    Put put =
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    Delete delete = new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
    boolean ret = jdbcService.mutate(Arrays.asList(put, delete), connection);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(anyList());
    verify(queryBuilder).upsertInto(any(), any(), any());
    verify(queryBuilder).deleteFrom(any(), any(), any());
  }
}
