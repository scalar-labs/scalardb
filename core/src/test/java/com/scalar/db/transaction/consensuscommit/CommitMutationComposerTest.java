package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.ScalarDbUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CommitMutationComposerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID = "id";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ANY_NAME_1, DataType.TEXT)
          .addColumn(ANY_NAME_2, DataType.TEXT)
          .addColumn(ANY_NAME_3, DataType.INT)
          .addPartitionKey(ANY_NAME_1)
          .addClusteringKey(ANY_NAME_2)
          .build();

  @Mock private TransactionTableMetadataManager tableMetadataManager;

  private CommitMutationComposer composer;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    composer = new CommitMutationComposer(ANY_ID, ANY_TIME_2, tableMetadataManager);

    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_1);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private TransactionResult prepareResult(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID)))
            .put(
                Attribute.PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toPreparedAtValue(ANY_TIME_1)))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(Attribute.toStateValue(state)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(2)))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void add_PutAndPreparedResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    Put put = preparePut();
    TransactionResult result = prepareResult(TransactionState.PREPARED);

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(put.getPartitionKey(), put.getClusteringKey().orElse(null))
            .forNamespace(put.forNamespace().get())
            .forTable(put.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValue(Attribute.toCommittedAtValue(ANY_TIME_2));
    expected.withValue(Attribute.toStateValue(TransactionState.COMMITTED));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_PutAndNullResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    Put put = preparePut();

    // Act
    composer.add(put, null); // result is not used, so it's set null

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(put.getPartitionKey(), put.getClusteringKey().orElse(null))
            .forNamespace(put.forNamespace().get())
            .forTable(put.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValue(Attribute.toCommittedAtValue(ANY_TIME_2));
    expected.withValue(Attribute.toStateValue(TransactionState.COMMITTED));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_DeleteAndDeletedResultGiven_ShouldComposeDeleteWithDeleteIfCondition()
      throws ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    TransactionResult result = prepareResult(TransactionState.DELETED);

    // Act
    composer.add(delete, result);

    // Assert
    Delete actual = (Delete) composer.get().get(0);
    delete.withConsistency(Consistency.LINEARIZABLE);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    assertThat(actual).isEqualTo(delete);
  }

  @Test
  public void add_DeleteAndNullResultGiven_ShouldComposeDeleteWithDeleteIfCondition()
      throws ExecutionException {
    // Arrange
    Delete delete = prepareDelete();

    // Act
    composer.add(delete, null); // result is not used, so it's set null

    // Assert
    Delete actual = (Delete) composer.get().get(0);
    delete.withConsistency(Consistency.LINEARIZABLE);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    assertThat(actual).isEqualTo(delete);
  }

  @Test
  public void add_SelectionAndPreparedResultGiven_ShouldComposePutForRollforward()
      throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = prepareResult(TransactionState.PREPARED);

    // Act
    composer.add(get, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().orElse(null))
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValue(Attribute.toCommittedAtValue(ANY_TIME_2));
    expected.withValue(Attribute.toStateValue(TransactionState.COMMITTED));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_SelectionAndDeletedResultGiven_ShouldComposeDeleteForRollforward()
      throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = prepareResult(TransactionState.DELETED);

    // Act
    composer.add(get, result);

    // Assert
    Delete actual = (Delete) composer.get().get(0);
    Delete expected =
        new Delete(get.getPartitionKey(), get.getClusteringKey().orElse(null))
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new DeleteIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    assertThat(actual).isEqualTo(expected);
  }
}
