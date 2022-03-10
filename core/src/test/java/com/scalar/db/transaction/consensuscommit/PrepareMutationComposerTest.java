package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toVersionValue;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ResultImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class PrepareMutationComposerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_ID_3 = "id3";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final long ANY_TIME_4 = 400;
  private static final long ANY_TIME_5 = 500;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_INT_3 = 300;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionalTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  private PrepareMutationComposer composer;
  private List<Mutation> mutations;

  @Before
  public void setUp() {
    mutations = new ArrayList<>();
    composer = new PrepareMutationComposer(ANY_ID_3, mutations, ANY_TIME_5);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_3);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private TransactionResult prepareResult() {
    ImmutableMap<String, Optional<Value<?>>> values =
        ImmutableMap.<String, Optional<Value<?>>>builder()
            .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
            .put(ANY_NAME_3, Optional.of(new IntValue(ANY_NAME_3, ANY_INT_2)))
            .put(Attribute.ID, Optional.of(Attribute.toIdValue(ANY_ID_2)))
            .put(Attribute.PREPARED_AT, Optional.of(Attribute.toPreparedAtValue(ANY_TIME_3)))
            .put(Attribute.COMMITTED_AT, Optional.of(Attribute.toCommittedAtValue(ANY_TIME_4)))
            .put(Attribute.STATE, Optional.of(Attribute.toStateValue(TransactionState.COMMITTED)))
            .put(Attribute.VERSION, Optional.of(Attribute.toVersionValue(2)))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                Optional.of(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1)))
            .put(Attribute.BEFORE_ID, Optional.of(Attribute.toBeforeIdValue(ANY_ID_1)))
            .put(
                Attribute.BEFORE_PREPARED_AT,
                Optional.of(Attribute.toBeforePreparedAtValue(ANY_TIME_1)))
            .put(
                Attribute.BEFORE_COMMITTED_AT,
                Optional.of(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)))
            .put(
                Attribute.BEFORE_STATE,
                Optional.of(Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
            .put(Attribute.BEFORE_VERSION, Optional.of(Attribute.toBeforeVersionValue(1)))
            .build();
    return new TransactionResult(new ResultImpl(values, TABLE_METADATA));
  }

  @Test
  public void add_PutAndResultGiven_ShouldComposePutWithPutIfCondition() {
    // Arrange
    Put put = preparePut();
    TransactionResult result = prepareResult();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) mutations.get(0);
    put.withConsistency(Consistency.LINEARIZABLE);
    put.withCondition(
        new PutIf(
            new ConditionalExpression(VERSION, toVersionValue(2), Operator.EQ),
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ)));
    put.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    put.withValue(Attribute.toIdValue(ANY_ID_3));
    put.withValue(Attribute.toStateValue(TransactionState.PREPARED));
    put.withValue(Attribute.toVersionValue(3));
    put.withValue(Attribute.toBeforePreparedAtValue(ANY_TIME_3));
    put.withValue(Attribute.toBeforeCommittedAtValue(ANY_TIME_4));
    put.withValue(Attribute.toBeforeIdValue(ANY_ID_2));
    put.withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    put.withValue(Attribute.toBeforeVersionValue(2));
    put.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2);
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void add_PutAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition() {
    // Arrange
    Put put = preparePut();

    // Act
    composer.add(put, null);

    // Assert
    Put actual = (Put) mutations.get(0);
    put.withConsistency(Consistency.LINEARIZABLE);
    put.withCondition(new PutIfNotExists());
    put.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    put.withValue(Attribute.toIdValue(ANY_ID_3));
    put.withValue(Attribute.toStateValue(TransactionState.PREPARED));
    put.withValue(Attribute.toVersionValue(1));
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void delete_DeleteAndResultGiven_ShouldComposePutWithPutIfCondition() {
    // Arrange
    Delete delete = prepareDelete();
    TransactionResult result = prepareResult();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) mutations.get(0);
    Put expected =
        new Put(delete.getPartitionKey(), delete.getClusteringKey().orElse(null))
            .forNamespace(delete.forNamespace().get())
            .forTable(delete.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(VERSION, toVersionValue(2), Operator.EQ),
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ)));
    expected.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    expected.withValue(Attribute.toIdValue(ANY_ID_3));
    expected.withValue(Attribute.toStateValue(TransactionState.DELETED));
    expected.withValue(Attribute.toVersionValue(3));
    expected.withValue(Attribute.toBeforePreparedAtValue(ANY_TIME_3));
    expected.withValue(Attribute.toBeforeCommittedAtValue(ANY_TIME_4));
    expected.withValue(Attribute.toBeforeIdValue(ANY_ID_2));
    expected.withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    expected.withValue(Attribute.toBeforeVersionValue(2));
    expected.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void delete_DeleteAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition() {
    // Arrange
    Delete delete = prepareDelete();

    // Act
    composer.add(delete, null);

    // Assert
    Put actual = (Put) mutations.get(0);
    Put expected =
        new Put(delete.getPartitionKey(), delete.getClusteringKey().orElse(null))
            .forNamespace(delete.forNamespace().get())
            .forTable(delete.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(new PutIfNotExists());
    expected.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    expected.withValue(Attribute.toIdValue(ANY_ID_3));
    expected.withValue(Attribute.toStateValue(TransactionState.DELETED));
    expected.withValue(Attribute.toVersionValue(1));
    assertThat(actual).isEqualTo(expected);
  }
}
