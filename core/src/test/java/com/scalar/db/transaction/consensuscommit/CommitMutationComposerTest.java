package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
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
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_NAME_7 = "name7";
  private static final String ANY_NAME_8 = "name8";
  private static final String ANY_NAME_9 = "name9";
  private static final String ANY_NAME_10 = "name10";
  private static final String ANY_NAME_11 = "name11";
  private static final String ANY_NAME_12 = "name12";
  private static final String ANY_NAME_13 = "name13";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addColumn(ANY_NAME_4, DataType.BOOLEAN)
              .addColumn(ANY_NAME_5, DataType.BIGINT)
              .addColumn(ANY_NAME_6, DataType.FLOAT)
              .addColumn(ANY_NAME_7, DataType.DOUBLE)
              .addColumn(ANY_NAME_8, DataType.TEXT)
              .addColumn(ANY_NAME_9, DataType.BLOB)
              .addColumn(ANY_NAME_10, DataType.DATE)
              .addColumn(ANY_NAME_11, DataType.TIME)
              .addColumn(ANY_NAME_12, DataType.TIMESTAMP)
              .addColumn(ANY_NAME_13, DataType.TIMESTAMPTZ)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  @Mock private TransactionTableMetadataManager tableMetadataManager;

  private CommitMutationComposer composer;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    composer = new CommitMutationComposer(ANY_ID, ANY_TIME_2, tableMetadataManager);

    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_1);
  }

  private Delete prepareDelete() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
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
            .put(Attribute.ID, TextColumn.of(ID, ANY_ID))
            .put(Attribute.PREPARED_AT, BigIntColumn.of(PREPARED_AT, ANY_TIME_1))
            .put(Attribute.STATE, IntColumn.of(STATE, state.get()))
            .put(Attribute.VERSION, IntColumn.of(VERSION, 2))
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
        Put.newBuilder()
            .namespace(put.forNamespace().get())
            .table(put.forTable().get())
            .partitionKey(put.getPartitionKey())
            .clusteringKey(put.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build())
            .bigIntValue(COMMITTED_AT, ANY_TIME_2)
            .intValue(STATE, TransactionState.COMMITTED.get())
            .textValue(BEFORE_ID, null)
            .intValue(BEFORE_STATE, null)
            .intValue(BEFORE_VERSION, null)
            .bigIntValue(BEFORE_PREPARED_AT, null)
            .bigIntValue(BEFORE_COMMITTED_AT, null)
            .intValue(BEFORE_PREFIX + ANY_NAME_3, null)
            .booleanValue(BEFORE_PREFIX + ANY_NAME_4, null)
            .bigIntValue(BEFORE_PREFIX + ANY_NAME_5, null)
            .floatValue(BEFORE_PREFIX + ANY_NAME_6, null)
            .doubleValue(BEFORE_PREFIX + ANY_NAME_7, null)
            .textValue(BEFORE_PREFIX + ANY_NAME_8, null)
            .blobValue(BEFORE_PREFIX + ANY_NAME_9, (byte[]) null)
            .dateValue(BEFORE_PREFIX + ANY_NAME_10, null)
            .timeValue(BEFORE_PREFIX + ANY_NAME_11, null)
            .timestampValue(BEFORE_PREFIX + ANY_NAME_12, null)
            .timestampTZValue(BEFORE_PREFIX + ANY_NAME_13, null)
            .build();
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
        ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
            .and(ConditionBuilder.column(STATE).isEqualToInt(TransactionState.PREPARED.get()))
            .build());
    expected.withBigIntValue(Attribute.COMMITTED_AT, ANY_TIME_2);
    expected.withIntValue(Attribute.STATE, TransactionState.COMMITTED.get());
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
        ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
            .and(ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
            .build());
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
        ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
            .and(ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
            .build());
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
        Put.newBuilder()
            .namespace(get.forNamespace().get())
            .table(get.forTable().get())
            .partitionKey(get.getPartitionKey())
            .clusteringKey(get.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build())
            .bigIntValue(COMMITTED_AT, ANY_TIME_2)
            .intValue(STATE, TransactionState.COMMITTED.get())
            .textValue(BEFORE_ID, null)
            .intValue(BEFORE_STATE, null)
            .intValue(BEFORE_VERSION, null)
            .bigIntValue(BEFORE_PREPARED_AT, null)
            .bigIntValue(BEFORE_COMMITTED_AT, null)
            .intValue(BEFORE_PREFIX + ANY_NAME_3, null)
            .booleanValue(BEFORE_PREFIX + ANY_NAME_4, null)
            .bigIntValue(BEFORE_PREFIX + ANY_NAME_5, null)
            .floatValue(BEFORE_PREFIX + ANY_NAME_6, null)
            .doubleValue(BEFORE_PREFIX + ANY_NAME_7, null)
            .textValue(BEFORE_PREFIX + ANY_NAME_8, null)
            .blobValue(BEFORE_PREFIX + ANY_NAME_9, (byte[]) null)
            .dateValue(BEFORE_PREFIX + ANY_NAME_10, null)
            .timeValue(BEFORE_PREFIX + ANY_NAME_11, null)
            .timestampValue(BEFORE_PREFIX + ANY_NAME_12, null)
            .timestampTZValue(BEFORE_PREFIX + ANY_NAME_13, null)
            .build();
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
        ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID))
            .and(ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
            .build());
    assertThat(actual).isEqualTo(expected);
  }
}
