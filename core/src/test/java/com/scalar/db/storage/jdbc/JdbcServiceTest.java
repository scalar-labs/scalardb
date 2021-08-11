package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpdateQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
public class JdbcServiceTest {

  private static final Optional<String> NAMESPACE = Optional.of("s1");
  private static final Optional<String> TABLE = Optional.of("t1");

  @Mock private QueryBuilder queryBuilder;
  @Mock private OperationChecker operationChecker;
  @Mock private JdbcTableMetadataManager tableMetadataManager;

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

  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JdbcService jdbcService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    jdbcService =
        new JdbcService(tableMetadataManager, operationChecker, queryBuilder, Optional.empty());

    // Arrange
    when(tableMetadataManager.getTableMetadata(any(Operation.class)))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn("p1", DataType.TEXT)
                .addColumn("v1", DataType.TEXT)
                .addPartitionKey("p1")
                .build());
  }

  @Test
  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  public void whenGetOperationExecuted_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.from(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);
    when(selectQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Get get = new Get(new Key("p1", "val"));
    jdbcService.get(get, connection, NAMESPACE, TABLE);

    // Assert
    verify(operationChecker).check(any(Get.class));
    verify(queryBuilder).select(any());
  }

  @Test
  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  public void whenGetScannerExecuted_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any(), anyBoolean(), any(), anyBoolean()))
        .thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(selectQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan = new Scan(new Key("p1", "val"));
    jdbcService.getScanner(scan, connection, NAMESPACE, TABLE);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
  }

  @Test
  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  public void whenScanExecuted_shouldCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);

    when(selectQueryBuilder.from(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any(), anyBoolean(), any(), anyBoolean()))
        .thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.orderBy(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.limit(anyInt())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);

    when(selectQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Scan scan = new Scan(new Key("p1", "val"));
    jdbcService.scan(scan, connection, NAMESPACE, TABLE);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenPutOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.upsertInto(any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Put put = new Put(new Key("p1", "val1")).withValue("v1", "val2");
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).upsertInto(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Put put =
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Put put =
        new Put(new Key("p1", "val1")).withValue("v1", "val2").withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        new Put(new Key("p1", "val1")).withValue("v1", "val2").withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void
      whenPutOperationWithPutIfNotExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.insertInto(any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(insertQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Put put =
        new Put(new Key("p1", "val1")).withValue("v1", "val2").withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).insertInto(any(), any());
  }

  @Test
  public void
      whenPutOperationWithPutIfNotExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.insertInto(any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(insertQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenThrow(sqlException);
    when(sqlException.getSQLState()).thenReturn("23000");

    // Act
    Put put =
        new Put(new Key("p1", "val1")).withValue("v1", "val2").withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).insertInto(any(), any());
  }

  @Test
  public void whenDeleteOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Delete delete = new Delete(new Key("p1", "val1"));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete =
        new Delete(new Key("p1", "val1"))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfConditionFails_shouldReturnFalseAndCallQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete =
        new Delete(new Key("p1", "val1"))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfExistsConditionExecuted_shouldReturnTrueAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete = new Delete(new Key("p1", "val1")).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfExistsConditionFails_shouldReturnFalseAndCallQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete = new Delete(new Key("p1", "val1")).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void whenMutateOperationExecuted_shouldReturnTrueAndCallQueryBuilder() throws Exception {
    // Arrange
    when(queryBuilder.upsertInto(any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Put put = new Put(new Key("p1", "val1")).withValue("v1", "val2");
    Delete delete = new Delete(new Key("p1", "val1"));
    boolean ret = jdbcService.mutate(Arrays.asList(put, delete), connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(anyList());
    verify(operationChecker).check(any(Put.class));
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).upsertInto(any(), any());
    verify(queryBuilder).deleteFrom(any(), any());
  }
}
