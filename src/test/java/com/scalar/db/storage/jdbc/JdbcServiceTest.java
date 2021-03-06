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
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcServiceTest {

  private static final Optional<String> NAMESPACE = Optional.of("s1");
  private static final Optional<String> TABLE = Optional.of("t1");

  @Mock private QueryBuilder queryBuilder;
  @Mock private TableMetadataManager tableMetadataManager;

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
    jdbcService = new JdbcService(tableMetadataManager, queryBuilder, Optional.empty());

    // Arrange
    when(tableMetadataManager.getTableMetadata(any()))
        .thenReturn(
            new JdbcTableMetadata(
                NAMESPACE.get(),
                TABLE.get(),
                Collections.singletonList("p1"),
                Collections.emptyList(),
                new HashMap<String, Scan.Ordering.Order>() {},
                new HashMap<String, DataType>() {
                  {
                    put("p1", DataType.TEXT);
                    put("v1", DataType.TEXT);
                  }
                },
                Collections.emptyList(),
                new HashMap<String, Scan.Ordering.Order>() {}));
  }

  @Test
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
    Get get = new Get(new Key(new TextValue("p1", "val")));
    jdbcService.get(get, connection, NAMESPACE, TABLE);

    // Assert
    verify(queryBuilder).select(any());
  }

  @Test
  public void whenScanOperationExecuted_shouldCallQueryBuilder() throws Exception {
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
    jdbcService.scan(scan, connection, NAMESPACE, TABLE);

    // Assert
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
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(
                new PutIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Put(new Key(new TextValue("p1", "val1")))
            .withValue(new TextValue("v1", "val2"))
            .withCondition(new PutIfNotExists());
    boolean ret = jdbcService.put(put, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
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
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Delete(new Key(new TextValue("p1", "val1")))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
        new Delete(new Key(new TextValue("p1", "val1")))
            .withCondition(
                new DeleteIf(
                    new ConditionalExpression(
                        "v1", new TextValue("val2"), ConditionalExpression.Operator.EQ)));
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
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
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1"))).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
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
    Delete delete =
        new Delete(new Key(new TextValue("p1", "val1"))).withCondition(new DeleteIfExists());
    boolean ret = jdbcService.delete(delete, connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isFalse();
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
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    boolean ret = jdbcService.mutate(Arrays.asList(put, delete), connection, NAMESPACE, TABLE);

    // Assert
    assertThat(ret).isTrue();
    verify(queryBuilder).upsertInto(any(), any());
    verify(queryBuilder).deleteFrom(any(), any());
  }
}
