package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.jdbc.checker.OperationChecker;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpdateQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcServiceTest {

  private static final Optional<String> NAMESPACE = Optional.of("s1");
  private static final Optional<String> TABLE_NAME = Optional.of("t1");

  @Mock private OperationChecker operationChecker;
  @Mock private QueryBuilder queryBuilder;

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
    jdbcService = new JdbcService(operationChecker, queryBuilder, Optional.empty());
  }

  @Test
  public void whenGetOperationExecuted_shouldCallOperationCheckerAndQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.select(any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.from(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.where(any(), any())).thenReturn(selectQueryBuilder);
    when(selectQueryBuilder.build()).thenReturn(selectQuery);
    when(selectQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    // Act
    Get get = new Get(new Key(new TextValue("p1", "val")));
    jdbcService.get(get, connection, NAMESPACE, TABLE_NAME);

    // Assert
    verify(operationChecker).check(any(Get.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenScanOperationExecuted_shouldCallOperationCheckerAndQueryBuilder()
      throws Exception {
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
    Scan scan = new Scan(new Key(new TextValue("p1", "val")));
    jdbcService.scan(scan, connection, NAMESPACE, TABLE_NAME);

    // Assert
    verify(operationChecker).check(any(Scan.class));
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenPutOperationExecuted_shouldCallOperationCheckerAndQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.upsertInto(any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.values(any(), any(), any())).thenReturn(upsertQueryBuilder);
    when(upsertQueryBuilder.build()).thenReturn(upsertQuery);
    when(upsertQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).upsertInto(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionExecuted_shouldCallOperationCheckerAndQueryBuilder()
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfConditionFails_shouldReturnFalse() throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void
      whenPutOperationWithPutIfExistsConditionExecuted_shouldCallOperationCheckerAndQueryBuilder()
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).update(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfExistsConditionFails_shouldReturnFalse() throws Exception {
    // Arrange
    when(queryBuilder.update(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.set(any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.where(any(), any())).thenReturn(updateQueryBuilder);
    when(updateQueryBuilder.build()).thenReturn(updateQuery);
    when(updateQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Put put =
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void
      whenPutOperationWithPutIfNotExistsConditionExecuted_shouldCallOperationCheckerAndQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.insertInto(any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.values(any(), any(), any())).thenReturn(insertQueryBuilder);
    when(insertQueryBuilder.build()).thenReturn(insertQuery);
    when(insertQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Put put =
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).insertInto(any(), any());
  }

  @Test
  public void whenPutOperationWithPutIfNotExistsConditionFails_shouldReturnFalse()
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void whenDeleteOperationExecuted_shouldCallOperationCheckerAndQueryBuilder()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);

    // Act
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfConditionExecuted_shouldCallOperationCheckerAndQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1")))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfConditionFails_shouldReturnFalse() throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1")))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void
      whenDeleteOperationWithDeleteIfExistsConditionExecuted_shouldCallOperationCheckerAndQueryBuilder()
          throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(1);

    // Act
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1"))).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }

  @Test
  public void whenDeleteOperationWithDeleteIfExistsConditionFails_shouldReturnFalse()
      throws Exception {
    // Arrange
    when(queryBuilder.deleteFrom(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.where(any(), any())).thenReturn(deleteQueryBuilder);
    when(deleteQueryBuilder.build()).thenReturn(deleteQuery);
    when(deleteQuery.prepareAndBind(any())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(0);

    // Act
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1"))).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void whenMutateOperationExecuted_shouldCallOperationCheckerAndQueryBuilder()
      throws Exception {
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
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    boolean ret = jdbcService.mutate(Arrays.asList(put, delete), connection, NAMESPACE, TABLE_NAME);

    // Assert
    assertThat(ret).isTrue();
    verify(operationChecker).check(any(Put.class));
    verify(queryBuilder).upsertInto(any(), any());
    verify(operationChecker).check(any(Delete.class));
    verify(queryBuilder).deleteFrom(any(), any());
  }
}
