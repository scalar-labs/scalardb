package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ResultImplTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final byte[] ANY_BLOB = "ブロブ".getBytes(StandardCharsets.UTF_8);
  private static final String ANY_COLUMN_NAME_1 = "val1";
  private static final String ANY_COLUMN_NAME_2 = "val2";
  private static final String ANY_COLUMN_NAME_3 = "val3";
  private static final String ANY_COLUMN_NAME_4 = "val4";
  private static final String ANY_COLUMN_NAME_5 = "val5";
  private static final String ANY_COLUMN_NAME_6 = "val6";
  private static final String ANY_COLUMN_NAME_7 = "val7";

  private Get get;
  private DynamoTableMetadata metadata;
  private Map<String, AttributeValue> item = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    Map<String, AttributeValue> metadataMap = new HashMap<>();
    metadataMap.put("partitionKey", AttributeValue.builder().ss(ANY_NAME_1).build());
    metadataMap.put("clusteringKey", AttributeValue.builder().ss(ANY_NAME_2).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(ANY_NAME_1, AttributeValue.builder().s("text").build());
    columns.put(ANY_NAME_2, AttributeValue.builder().s("text").build());
    columns.put(ANY_COLUMN_NAME_1, AttributeValue.builder().s("boolean").build());
    columns.put(ANY_COLUMN_NAME_2, AttributeValue.builder().s("int").build());
    columns.put(ANY_COLUMN_NAME_3, AttributeValue.builder().s("bigint").build());
    columns.put(ANY_COLUMN_NAME_4, AttributeValue.builder().s("float").build());
    columns.put(ANY_COLUMN_NAME_5, AttributeValue.builder().s("double").build());
    columns.put(ANY_COLUMN_NAME_6, AttributeValue.builder().s("text").build());
    columns.put(ANY_COLUMN_NAME_7, AttributeValue.builder().s("blob").build());
    metadataMap.put("columns", AttributeValue.builder().m(columns).build());
    metadata = new DynamoTableMetadata(metadataMap);

    item.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_2, AttributeValue.builder().s(ANY_TEXT_2).build());
    item.put(ANY_COLUMN_NAME_1, AttributeValue.builder().bool(true).build());
    item.put(
        ANY_COLUMN_NAME_2, AttributeValue.builder().n(String.valueOf(Integer.MAX_VALUE)).build());
    item.put(ANY_COLUMN_NAME_3, AttributeValue.builder().n(String.valueOf(Long.MAX_VALUE)).build());
    item.put(
        ANY_COLUMN_NAME_4, AttributeValue.builder().n(String.valueOf(Float.MAX_VALUE)).build());
    item.put(
        ANY_COLUMN_NAME_5, AttributeValue.builder().n(String.valueOf(Double.MAX_VALUE)).build());
    item.put(ANY_COLUMN_NAME_6, AttributeValue.builder().s("string").build());
    item.put(
        ANY_COLUMN_NAME_7, AttributeValue.builder().b(SdkBytes.fromByteArray(ANY_BLOB)).build());

    get = new Get(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(item, get, metadata);

    // Act
    Optional<Value> actual = result.getValue(ANY_NAME_1);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getValues_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(item, get, metadata);

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
    Map<String, AttributeValue> emptyItem = new HashMap<>();
    ResultImpl result = new ResultImpl(emptyItem, get, metadata);

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
  public void getValues_NullValuesGivenInConstructor_ShouldReturnDefaultValueSet() {
    // Arrange
    Map<String, AttributeValue> nullItem = new HashMap<>();
    nullItem.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    nullItem.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    nullItem.put(ANY_NAME_2, AttributeValue.builder().s(ANY_TEXT_2).build());
    nullItem.put(ANY_COLUMN_NAME_1, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_2, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_3, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_4, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_5, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_6, AttributeValue.builder().nul(true).build());
    nullItem.put(ANY_COLUMN_NAME_7, AttributeValue.builder().nul(true).build());

    ResultImpl result = new ResultImpl(nullItem, get, metadata);

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
    ResultImpl spy = spy(new ResultImpl(item, get, metadata));
    spy.interpret(item, metadata);
    spy.getValue(ANY_COLUMN_NAME_1);

    // Act
    spy.getValue(ANY_COLUMN_NAME_1);

    // Assert
    verify(spy, times(1)).interpret(item, metadata);
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl result = new ResultImpl(item, get, metadata);
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
    ResultImpl result = new ResultImpl(item, getWithProjections, metadata);

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
    ResultImpl r1 = new ResultImpl(item, get, metadata);
    ResultImpl r2 = new ResultImpl(item, get, metadata);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(item, get, metadata);
    Map<String, AttributeValue> item2 = new HashMap<>();
    item2.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    item2.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    item2.put(ANY_NAME_2, AttributeValue.builder().s(ANY_TEXT_2).build());
    item2.put(ANY_COLUMN_NAME_1, AttributeValue.builder().bool(true).build());
    ResultImpl r2 = new ResultImpl(item2, get, metadata);

    // Act Assert
    assertThat(r1.equals(r2)).isFalse();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new ResultImpl(null, null, null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getPartitionKey_RequiredValuesGiven_ShouldReturnPartitionKey() {
    // Arrange
    ResultImpl result = new ResultImpl(item, get, metadata);

    // Act
    Optional<Key> key = result.getPartitionKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result = new ResultImpl(item, get, metadata);

    // Act
    Optional<Key> key = result.getClusteringKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
  }
}
