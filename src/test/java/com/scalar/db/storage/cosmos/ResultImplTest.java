package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Get;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class ResultImplTest {
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final byte[] ANY_BLOB = "ブロブ".getBytes(StandardCharsets.UTF_8);
  private static final String ANY_BLOB_STRING = Base64.getEncoder().encodeToString(ANY_BLOB);
  private static final String ANY_COLUMN_NAME_1 = "val1";
  private static final String ANY_COLUMN_NAME_2 = "val2";
  private static final String ANY_COLUMN_NAME_3 = "val3";
  private static final String ANY_COLUMN_NAME_4 = "val4";
  private static final String ANY_COLUMN_NAME_5 = "val5";
  private static final String ANY_COLUMN_NAME_6 = "val6";
  private static final String ANY_COLUMN_NAME_7 = "val7";
  private static final int ANY_INT = 10;

  private Record record;
  private Get get;
  private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    metadata = new TableMetadata();
    metadata.setId("ks.tbl");
    metadata.setPartitionKeyNames(ImmutableSet.of(ANY_NAME_1));
    metadata.setClusteringKeyNames(ImmutableSet.of(ANY_NAME_2));
    ImmutableMap<String, String> columns =
        ImmutableMap.<String, String>builder()
            .put(ANY_NAME_1, "text")
            .put(ANY_NAME_2, "text")
            .put(ANY_COLUMN_NAME_1, "boolean")
            .put(ANY_COLUMN_NAME_2, "int")
            .put(ANY_COLUMN_NAME_3, "bigint")
            .put(ANY_COLUMN_NAME_4, "float")
            .put(ANY_COLUMN_NAME_5, "double")
            .put(ANY_COLUMN_NAME_6, "text")
            .put(ANY_COLUMN_NAME_7, "blob")
            .build();
    metadata.setColumns(columns);

    record = new Record();
    record.setId(ANY_ID_1);
    record.setConcatenatedPartitionKey(ANY_TEXT_1);
    record.setPartitionKey(ImmutableMap.of(ANY_NAME_1, ANY_TEXT_1));
    record.setClusteringKey(ImmutableMap.of(ANY_NAME_2, ANY_TEXT_2));
    ImmutableMap<String, Object> values =
        ImmutableMap.<String, Object>builder()
            .put(ANY_COLUMN_NAME_1, true)
            .put(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)
            .put(ANY_COLUMN_NAME_3, Long.MAX_VALUE)
            .put(ANY_COLUMN_NAME_4, Float.MAX_VALUE)
            .put(ANY_COLUMN_NAME_5, Double.MAX_VALUE)
            .put(ANY_COLUMN_NAME_6, "string")
            // Cosmos DB converts byte[] to a base64 string
            .put(ANY_COLUMN_NAME_7, ANY_BLOB_STRING)
            .build();
    record.setValues(values);

    get = new Get(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(record, get, metadata);

    // Act
    Optional<Value> actual = result.getValue(ANY_NAME_1);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getValues_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(record, get, metadata);

    // Act
    Map<String, Value> actual = result.getValues();

    // Assert
    assertThat(actual.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get(ANY_COLUMN_NAME_1)).isEqualTo(new BooleanValue(ANY_COLUMN_NAME_1, true));
    assertThat(actual.get(ANY_COLUMN_NAME_7)).isEqualTo(new BlobValue(ANY_COLUMN_NAME_7, ANY_BLOB));
  }

  @Test
  public void getValues_NoValuesGivenInConstructor_ShouldReturnDefaultValueSet() {
    // Arrange
    Record emptyRecord = new Record();
    emptyRecord.setId(ANY_ID_1);
    emptyRecord.setConcatenatedPartitionKey(ANY_TEXT_1);
    emptyRecord.setPartitionKey(ImmutableMap.of(ANY_NAME_1, ANY_TEXT_1));
    emptyRecord.setClusteringKey(ImmutableMap.of(ANY_NAME_2, ANY_TEXT_2));

    ResultImpl result = new ResultImpl(emptyRecord, get, metadata);

    // Act
    Map<String, Value> actual = result.getValues();

    // Assert
    assertThat(actual.get(ANY_COLUMN_NAME_1)).isEqualTo(new BooleanValue(ANY_COLUMN_NAME_1, false));
    assertThat(actual.get(ANY_COLUMN_NAME_2)).isEqualTo(new IntValue(ANY_COLUMN_NAME_2, 0));
    assertThat(actual.get(ANY_COLUMN_NAME_3)).isEqualTo(new BigIntValue(ANY_COLUMN_NAME_3, 0L));
    assertThat(actual.get(ANY_COLUMN_NAME_4)).isEqualTo(new FloatValue(ANY_COLUMN_NAME_4, 0.0f));
    assertThat(actual.get(ANY_COLUMN_NAME_5)).isEqualTo(new DoubleValue(ANY_COLUMN_NAME_5, 0.0));
    assertThat(actual.get(ANY_COLUMN_NAME_6))
        .isEqualTo(new TextValue(ANY_COLUMN_NAME_6, (String) null));
    assertThat(actual.get(ANY_COLUMN_NAME_7)).isEqualTo(new BlobValue(ANY_COLUMN_NAME_7, null));
  }

  @Test
  public void getValue_GetValueCalledBefore_ShouldNotLoadAgain() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(record, get, metadata));
    spy.interpret(record, get, metadata);
    spy.getValue(ANY_COLUMN_NAME_1);

    // Act
    spy.getValue(ANY_COLUMN_NAME_1);

    // Assert
    verify(spy, times(1)).interpret(record, get, metadata);
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl result = new ResultImpl(record, get, metadata);
    Map<String, Value> values = result.getValues();

    // Act Assert
    assertThatThrownBy(
            () -> {
              values.put("new", new TextValue(ANY_NAME_1, ANY_TEXT_1));
            })
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getValues_GetWithProjectionsGiven_ShouldReturnWhatsSet() {
    // Arrange
    Get getWithProjections =
        get.withProjections(Arrays.asList(ANY_COLUMN_NAME_1, ANY_COLUMN_NAME_4));
    ResultImpl result = new ResultImpl(record, getWithProjections, metadata);

    // Act
    Map<String, Value> actual = result.getValues();

    // Assert
    assertThat(actual.size()).isEqualTo(4);
    assertThat(actual.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get(ANY_COLUMN_NAME_1)).isEqualTo(new BooleanValue(ANY_COLUMN_NAME_1, true));
    assertThat(actual.get(ANY_COLUMN_NAME_4))
        .isEqualTo(new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE));
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    ResultImpl r1 = new ResultImpl(record, get, metadata);
    ResultImpl r2 = new ResultImpl(record, get, metadata);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(record, get, metadata);
    Record record2 = new Record();
    record2.setId(ANY_ID_2);
    record2.setConcatenatedPartitionKey(ANY_TEXT_1);
    record2.setPartitionKey(ImmutableMap.of(ANY_NAME_1, ANY_TEXT_1));
    record2.setClusteringKey(ImmutableMap.of(ANY_NAME_2, ANY_TEXT_2));
    ImmutableMap<String, Object> values =
        ImmutableMap.<String, Object>builder().put(ANY_COLUMN_NAME_1, true).build();
    record2.setValues(values);
    ResultImpl r2 = new ResultImpl(record2, get, metadata);

    // Act Assert
    assertThat(r1.equals(r2)).isFalse();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new ResultImpl((Record) null, null, null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getPartitionKey_RequiredValuesGiven_ShouldReturnPartitionKey() {
    // Arrange
    ResultImpl result = new ResultImpl(record, get, metadata);

    // Act
    Optional<Key> key = result.getPartitionKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result = new ResultImpl(record, get, metadata);

    // Act
    Optional<Key> key = result.getClusteringKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
  }
}
