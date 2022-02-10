package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

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
  private CommitMutationComposer composer;
  private List<Mutation> mutations;

  @Before
  public void setUp() {
    mutations = new ArrayList<>();
    composer = new CommitMutationComposer(ANY_ID, mutations, ANY_TIME_2);
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

  private void configureResult(Result mock, TransactionState state) {
    when(mock.getPartitionKey()).thenReturn(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
    when(mock.getClusteringKey()).thenReturn(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));

    ImmutableMap<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(ANY_NAME_3, new IntValue(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID))
            .put(Attribute.PREPARED_AT, Attribute.toPreparedAtValue(ANY_TIME_1))
            .put(Attribute.STATE, Attribute.toStateValue(state))
            .put(Attribute.VERSION, Attribute.toVersionValue(2))
            .build();

    when(mock.getValues()).thenReturn(values);
  }

  private TransactionResult prepareResult(TransactionState state) {
    Result result = mock(Result.class);
    configureResult(result, state);
    return new TransactionResult(result);
  }

  @Test
  public void add_PutGiven_ShouldComposePutWithPutIfCondition() {
    // Arrange
    Put put = preparePut();

    // Act
    composer.add(put, null); // result is not used, so it's set null

    // Assert
    Put actual = (Put) mutations.get(0);
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
  public void add_DeleteGiven_ShouldComposeDeleteWithDeleteIfCondition() {
    // Arrange
    Delete delete = prepareDelete();

    // Act
    composer.add(delete, null); // result is not used, so it's set null

    // Assert
    Delete actual = (Delete) mutations.get(0);
    delete.withConsistency(Consistency.LINEARIZABLE);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    assertThat(actual).isEqualTo(delete);
  }

  @Test
  public void add_SelectionAndPreparedResultGiven_ShouldComposePutForRollforward() {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = prepareResult(TransactionState.PREPARED);

    // Act
    composer.add(get, result);

    // Assert
    Put actual = (Put) mutations.get(0);
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
  public void add_SelectionAndDeletedResultGiven_ShouldComposePutForRollforward() {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = prepareResult(TransactionState.DELETED);

    // Act
    composer.add(get, result);

    // Assert
    Delete actual = (Delete) mutations.get(0);
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
