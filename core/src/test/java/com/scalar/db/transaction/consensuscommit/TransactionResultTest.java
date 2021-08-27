package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class TransactionResultTest {
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final long ANY_TIME_4 = 400;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_VERSION_1 = 1;
  private static final int ANY_VERSION_2 = 2;
  private TransactionResult result;

  private void configureResult(Result mock) {
    when(mock.getPartitionKey()).thenReturn(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
    when(mock.getClusteringKey()).thenReturn(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));

    ImmutableMap<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(ANY_NAME_3, new IntValue(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_2))
            .put(Attribute.PREPARED_AT, Attribute.toPreparedAtValue(ANY_TIME_3))
            .put(Attribute.COMMITTED_AT, Attribute.toCommittedAtValue(ANY_TIME_4))
            .put(Attribute.STATE, Attribute.toStateValue(TransactionState.COMMITTED))
            .put(Attribute.VERSION, Attribute.toVersionValue(ANY_VERSION_2))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(Attribute.BEFORE_ID, Attribute.toBeforeIdValue(ANY_ID_1))
            .put(Attribute.BEFORE_PREPARED_AT, Attribute.toBeforePreparedAtValue(ANY_TIME_1))
            .put(Attribute.BEFORE_COMMITTED_AT, Attribute.toBeforeCommittedAtValue(ANY_TIME_2))
            .put(Attribute.BEFORE_STATE, Attribute.toBeforeStateValue(TransactionState.COMMITTED))
            .put(Attribute.BEFORE_VERSION, Attribute.toBeforeVersionValue(ANY_VERSION_1))
            .build();

    when(mock.getValues()).thenReturn(values);
  }

  private TransactionResult prepareResult() {
    Result result = mock(Result.class);
    configureResult(result);
    return new TransactionResult(result);
  }

  @Test
  public void getPartitionKey_ResultGiven_ShouldReturnCorrectKey() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    Optional<Key> actual = result.getPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getClusteringKey_ResultGiven_ShouldReturnCorrectKey() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    Optional<Key> actual = result.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));
  }

  @Test
  public void getValue_ResultGiven_ShouldReturnCorrectValue() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    Optional<Value<?>> actual = result.getValue(ANY_NAME_3);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new IntValue(ANY_NAME_3, ANY_INT_2)));
  }

  @Test
  public void getValues_ValuesGiven_ShouldReturnCorrectValues() {
    // Arrange
    Result given = prepareResult();
    result = new TransactionResult(given);

    // Act
    Map<String, Value<?>> values = result.getValues();

    // Assert
    assertThat(values.size()).isEqualTo(given.getValues().size());
    given.getValues().forEach((k, v) -> assertThat(values.get(v.getName())).isEqualTo(v));
  }

  @Test
  public void equals_SameValuesGiven_ShouldReturnTrue() {
    // Arrange
    result = new TransactionResult(prepareResult());
    TransactionResult another = new TransactionResult(prepareResult());

    // Act
    boolean isEqual = result.equals(another);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    result = new TransactionResult(prepareResult());
    TransactionResult another = new TransactionResult(mock(Result.class));

    // Act
    boolean isEqual = result.equals(another);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void getTransactionId_ValuesGiven_ShouldReturnCorrectId() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    String id = result.getId();

    // Assert
    assertThat(id).isEqualTo(ANY_ID_2);
  }

  @Test
  public void getTransactionState_ValuesGiven_ShouldReturnCorrectState() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    TransactionState state = result.getState();

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getTransactionVersion_ValuesGiven_ShouldReturnCorrectVersion() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    int version = result.getVersion();

    // Assert
    assertThat(version).isEqualTo(ANY_VERSION_2);
  }

  @Test
  public void isCommitted_ValuesGiven_ShouldReturnCommitted() {
    // Arrange
    result = new TransactionResult(prepareResult());

    // Act
    boolean isCommitted = result.isCommitted();

    // Assert
    assertThat(isCommitted).isTrue();
  }
}
