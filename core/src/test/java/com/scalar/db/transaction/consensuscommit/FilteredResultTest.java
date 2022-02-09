package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ResultImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FilteredResultTest {

  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";

  private static final IntValue ACCOUNT_ID_VALUE = new IntValue(ACCOUNT_ID, 0);
  private static final IntValue ACCOUNT_TYPE_VALUE = new IntValue(ACCOUNT_TYPE, 1);
  private static final IntValue BALANCE_VALUE = new IntValue(BALANCE, 2);
  private static final TextValue ID_VALUE = new TextValue(Attribute.ID, "aaa");
  private static final IntValue STATE_VALUE = new IntValue(Attribute.STATE, 3);
  private static final IntValue VERSION_VALUE = new IntValue(Attribute.VERSION, 4);
  private static final BigIntValue PREPARED_AT_VALUE = new BigIntValue(Attribute.PREPARED_AT, 5);
  private static final BigIntValue COMMITTED_AT_VALUE = new BigIntValue(Attribute.COMMITTED_AT, 6);
  private static final IntValue BEFORE_BALANCE_VALUE =
      new IntValue(Attribute.BEFORE_PREFIX + BALANCE, 7);
  private static final TextValue BEFORE_ID_VALUE = new TextValue(Attribute.BEFORE_ID, "bbb");
  private static final IntValue BEFORE_STATE_VALUE = new IntValue(Attribute.BEFORE_STATE, 8);
  private static final IntValue BEFORE_VERSION_VALUE = new IntValue(Attribute.BEFORE_VERSION, 9);
  private static final BigIntValue BEFORE_PREPARED_AT_VALUE =
      new BigIntValue(Attribute.BEFORE_PREPARED_AT, 10);
  private static final BigIntValue BEFORE_COMMITTED_AT_VALUE =
      new BigIntValue(Attribute.BEFORE_COMMITTED_AT, 11);

  private Result result;

  @Before
  public void setUp() {
    // Arrange
    Map<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(ACCOUNT_ID, ACCOUNT_ID_VALUE)
            .put(ACCOUNT_TYPE, ACCOUNT_TYPE_VALUE)
            .put(BALANCE, BALANCE_VALUE)
            .put(Attribute.ID, ID_VALUE)
            .put(Attribute.STATE, STATE_VALUE)
            .put(Attribute.VERSION, VERSION_VALUE)
            .put(Attribute.PREPARED_AT, PREPARED_AT_VALUE)
            .put(Attribute.COMMITTED_AT, COMMITTED_AT_VALUE)
            .put(Attribute.BEFORE_PREFIX + BALANCE, BEFORE_BALANCE_VALUE)
            .put(Attribute.BEFORE_ID, BEFORE_ID_VALUE)
            .put(Attribute.BEFORE_STATE, BEFORE_STATE_VALUE)
            .put(Attribute.BEFORE_VERSION, BEFORE_VERSION_VALUE)
            .put(Attribute.BEFORE_PREPARED_AT, BEFORE_PREPARED_AT_VALUE)
            .put(Attribute.BEFORE_COMMITTED_AT, BEFORE_COMMITTED_AT_VALUE)
            .build();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    result = new ResultImpl(values, metadata);
  }

  @Test
  public void WithoutProjections_ShouldFilterOutTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult = new FilteredResult(result, Collections.emptyList());

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isPresent();
    assertThat(filteredResult.getClusteringKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getClusteringKey().get().get().get(0)).isEqualTo(ACCOUNT_TYPE_VALUE);

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).get()).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();
  }

  @Test
  public void WithProjections_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult = new FilteredResult(result, Arrays.asList(ACCOUNT_ID, BALANCE));

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isNotPresent();

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();
  }

  @Test
  public void
      WithProjectionsForPartitionKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(ACCOUNT_ID));

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isNotPresent();

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE)).isNotPresent();

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();
  }

  @Test
  public void
      WithProjectionsForClusteringKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(ACCOUNT_TYPE));

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isNotPresent();

    assertThat(filteredResult.getClusteringKey()).isPresent();
    assertThat(filteredResult.getClusteringKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getClusteringKey().get().get().get(0)).isEqualTo(ACCOUNT_TYPE_VALUE);

    assertThat(filteredResult.getValue(ACCOUNT_ID)).isNotPresent();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).get()).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(BALANCE)).isNotPresent();

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();
  }

  @Test
  public void
      WithProjectionsForNonPrimaryKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult = new FilteredResult(result, Collections.singletonList(BALANCE));

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isNotPresent();
    assertThat(filteredResult.getClusteringKey()).isNotPresent();

    assertThat(filteredResult.getValue(ACCOUNT_ID)).isNotPresent();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();
  }
}
